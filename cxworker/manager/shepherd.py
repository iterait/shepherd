import logging
from zmq.error import ZMQError
import zmq.green as zmq
from typing import Mapping, Generator, Tuple, Dict, Any, Optional, List
from minio import Minio
import gevent
import os.path as path
from functools import partial
import shutil
from io import BytesIO

from cxworker.api.models import SheepModel
from cxworker.sheep.sheep import DockerSheep, BareSheep, BaseSheep, DummySheep
from cxworker.errors import SheepConfigurationError
from ..api.errors import UnknownSheepError, UnknownJobError
from .config import RegistryConfig
from ..utils import pull_minio_bucket, push_minio_bucket, minio_object_exists
from cxworker.utils import create_clean_dir


class Shepherd:
    """
    Manages creation and access to a configured set of sheep
    """

    def __init__(self, zmq_context: zmq.Context, registry_config: Optional[RegistryConfig],
                 sheep_config: Mapping[str, Dict[str, Any]], data_root: str, minio: Minio):
        """
        Create Shepherd, the mighty Sheep manager and his sheep.

        :param zmq_context: zmq context
        :param registry_config: optional docker registry config
        :param sheep_config: sheep config
        :param data_root: directory where the task/sheep directories will be managed
        :param minio: Minio handle
        """
        self.minio = minio
        self.poller = zmq.Poller()
        self.sheep: Dict[str, BaseSheep] = {}

        self.notifier = zmq_context.socket(zmq.PUB)
        self.notifier.bind("tcp://*:6666")

        for sheep_id, config in sheep_config.items():
            socket = zmq_context.socket(zmq.DEALER)
            sheep_type = config["type"]
            sheep_data_root = create_clean_dir(path.join(data_root, sheep_id))
            common_kwargs = {'socket': socket, 'sheep_data_root': sheep_data_root}

            if sheep_type == "docker":
                if registry_config is None:
                    raise SheepConfigurationError("To use docker sheep, you need to configure a registry URL")
                sheep = DockerSheep(config=config, registry_config=registry_config, **common_kwargs)
            elif sheep_type == "bare":
                sheep = BareSheep(config=config, **common_kwargs)
            elif sheep_type == "dummy":
                sheep = DummySheep(config=config, **common_kwargs)
            else:
                raise SheepConfigurationError("Unknown sheep type: {}".format(sheep_type))

            logging.info('Created sheep `%s` of type `%s`', sheep_id, sheep_type)
            self.sheep[sheep_id] = sheep
            self.poller.register(socket, zmq.POLLIN)

    def __getitem__(self, sheep_id: str) -> BaseSheep:
        """
        Get the sheep with the given ``sheep_id``.

        :param sheep_id: sheep id
        :return: sheep with the given ``sheep_id``
        :raise UnknownContainerError: if the given ``sheep_id`` is not known to this registry
        """
        if sheep_id not in self.sheep:
            raise UnknownSheepError('Unknown sheep id `{}`'.format(sheep_id))
        return self.sheep[sheep_id]

    def start_sheep(self, sheep_id: str, model: str, version: str) -> None:
        logging.info('Starting sheep `%s` with model `%s:%s`', sheep_id, model, version)
        sheep = self[sheep_id]

        if sheep.running:
            self.slaughter_sheep(sheep_id)

        sheep.load_model(model, version)
        sheep.start()
        sheep.requests_set = set()
        sheep.socket.connect("tcp://0.0.0.0:{}".format(sheep.config.port))

    def refresh_model(self, sheep_id: str) -> None:
        sheep = self[sheep_id]
        # If there is an update, restart the sheep
        if sheep.update_model():
            self.start_sheep(sheep_id, sheep.model_name, sheep.model_version)

    def slaughter_sheep(self, sheep_id: str) -> None:
        logging.info('Slaughtering sheep `%s`', sheep_id)
        sheep = self[sheep_id]
        zmq_address = "tcp://0.0.0.0:{}".format(sheep.config.port)
        try:
            sheep.socket.disconnect(zmq_address)
        except ZMQError:
            logging.warning('Failed to disconnect socket of `{}`  (perhaps it was not started/connected)'
                            .format(sheep_id))
        sheep.slaughter()

    def enqueue_job(self, sheep_id: str, job_id: str) -> None:
        logging.debug('En-queueing job `%s` for sheep `%s`', job_id, sheep_id)
        self[sheep_id].requests_queue.put(job_id)
        self[sheep_id].requests_set.add(job_id)

    def dequeue_and_feed_jobs(self, sheep_id: str) -> None:
        while True:
            sheep = self[sheep_id]
            job_id = sheep.requests_queue.get()
            logging.info('De-queueing job `%s` for sheep `%s`', job_id, sheep_id)
            working_directory = create_clean_dir(path.join(sheep.sheep_data_root, job_id))
            pull_minio_bucket(self.minio, job_id, working_directory)
            create_clean_dir(path.join(working_directory, 'outputs'))
            self[sheep_id].socket.send_multipart([b"input", job_id.encode(), sheep.sheep_data_root.encode()])

    def spawn_feeders(self) -> List[gevent.Greenlet]:
        dequeuers = []
        for sheep_id in self.sheep.keys():
            dequeue_jobs_fn = partial(self.dequeue_and_feed_jobs, sheep_id)
            dequeuers.append(gevent.spawn(dequeue_jobs_fn))
        return dequeuers

    def listen(self) -> None:
        while True:
            result = self.poller.poll()
            sheep_ids = (sheep_id for sheep_id, sheep in self.sheep.items() if (sheep.socket, zmq.POLLIN) in result)
            for sheep_id in sheep_ids:
                message_type, job_id, *rest = self[sheep_id].socket.recv_multipart()
                message_type = message_type.decode()
                job_id = job_id.decode()

                working_directory = path.join(self[sheep_id].sheep_data_root, job_id)
                push_minio_bucket(self.minio, job_id, working_directory)
                shutil.rmtree(working_directory)

                if message_type == "output":
                    self.minio.put_object(job_id, 'done', BytesIO(b''), 0)
                else:
                    self.minio.put_object(job_id, 'error', BytesIO(b''), 0)
                    logging.error("The sheep encountered an error when working on job `%s`", job_id)
                    if len(rest) >= 1:
                        logging.error("Received error traceback:")
                        logging.error(rest[0])

                self[sheep_id].requests_set.remove(job_id)
                logging.info('Job `%s` from sheep `%s` has been finished', job_id, sheep_id)
                self.notifier.send(b'')

    def get_status(self) -> Generator[Tuple[str, SheepModel], None, None]:
        """
        Get status information for all sheep

        :return: a generator of status information
        """
        for sheep_id, sheep in self.sheep.items():
            yield sheep_id, SheepModel({
                "running": sheep.running,
                "model": {
                    "name": sheep.model_name,
                    "version": sheep.model_version
                }
            })

    def slaughter_all(self) -> None:
        for sheep_id, sheep in self.sheep.items():
            if sheep.running is not None:
                self.slaughter_sheep(sheep_id)

    def is_job_done(self, job_id: str) -> bool:
        if minio_object_exists(self.minio, job_id, 'done') or minio_object_exists(self.minio, job_id, 'error'):
            return True
        else:
            for sheep in self.sheep.values():
                if job_id in sheep.requests_set:
                    return False
            else:
                raise UnknownJobError('Job `{}` is not know to this worker'.format(job_id))

