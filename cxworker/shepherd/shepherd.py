import logging
import zmq.green as zmq
from typing import Mapping, Generator, Tuple, Dict, Any, Optional
from minio import Minio
import gevent
import os.path as path
from functools import partial
import shutil
from io import BytesIO

from cxworker.api.models import SheepModel
from cxworker.sheep import *
from cxworker.errors import SheepConfigurationError
from ..api.errors import UnknownSheepError, UnknownJobError
from .config import RegistryConfig
from ..utils import pull_minio_bucket, push_minio_bucket, minio_object_exists
from cxworker.utils import create_clean_dir
from ..comm import Messenger, InputMessage, DoneMessage, ErrorMessage, JobDoneNotifier
from ..api.models import ModelModel


class Shepherd:
    """
    Manages creation and access to a configured set of sheep
    """

    def __init__(self, zmq_context: zmq.Context, registry_config: Optional[RegistryConfig],
                 sheep_config: Mapping[str, Dict[str, Any]], data_root: str, minio: Minio):
        """
        Create Shepherd, the mighty Sheep shepherd and his sheep.

        :param zmq_context: zmq context
        :param registry_config: optional docker registry config
        :param sheep_config: sheep config
        :param data_root: directory where the task/sheep directories will be managed
        :param minio: Minio handle
        """
        self.minio = minio
        self.poller = zmq.Poller()
        self.sheep: Dict[str, BaseSheep] = {}
        self.notifier = JobDoneNotifier()

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
            else:
                raise SheepConfigurationError("Unknown sheep type: {}".format(sheep_type))

            logging.info('Created sheep `%s` of type `%s`', sheep_id, sheep_type)
            self.sheep[sheep_id] = sheep
            self.poller.register(socket, zmq.POLLIN)
            gevent.spawn(partial(self.dequeue_and_feed_jobs, sheep_id))

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
        self[sheep_id].start(model, version)

    def slaughter_sheep(self, sheep_id: str) -> None:
        logging.info('Slaughtering sheep `%s`', sheep_id)
        self[sheep_id].slaughter()

    def enqueue_job(self, job_id: str, job_meta: ModelModel, sheep_id: Optional[str]=None) -> None:
        logging.info('En-queueing job `%s` for sheep `%s`', job_id, sheep_id)
        if sheep_id is None:
            sheep_id = next(iter(self.sheep.keys()))
            logging.info('Job `%s` is auto-assigned to sheep `%s`', job_id, sheep_id)
        self[sheep_id].jobs_meta[job_id] = job_meta
        self[sheep_id].jobs_queue.put(job_id)

    def dequeue_and_feed_jobs(self, sheep_id: str) -> None:
        while True:
            sheep = self[sheep_id]
            job_id = sheep.jobs_queue.get()
            logging.info('De-queueing job `%s` for sheep `%s`', job_id, sheep_id)
            working_directory = create_clean_dir(path.join(sheep.sheep_data_root, job_id))
            pull_minio_bucket(self.minio, job_id, working_directory)
            create_clean_dir(path.join(working_directory, 'outputs'))
            model = sheep.jobs_meta[job_id]
            if model.name != sheep.model_name or model.version != sheep.model_version:
                logging.info('Job `%s` requires model `%s:%s` on `%s`', job_id, model.name, model.version, sheep_id)
                self.notifier.wait_for(lambda: len(sheep.in_progress) == 0)
                self.slaughter_sheep(sheep_id)
                self.start_sheep(sheep_id, model.name, model.version)

            Messenger.send(sheep.socket, InputMessage(dict(job_id=job_id, io_data_root=sheep.sheep_data_root)))
            sheep.in_progress.add(job_id)

    def listen(self) -> None:
        while True:
            result = self.poller.poll()
            sheep_ids = (sheep_id for sheep_id, sheep in self.sheep.items() if (sheep.socket, zmq.POLLIN) in result)
            for sheep_id in sheep_ids:
                sheep = self[sheep_id]
                message = Messenger.recv(sheep.socket, [DoneMessage, ErrorMessage])
                job_id = message.job_id

                working_directory = path.join(self[sheep_id].sheep_data_root, job_id)
                push_minio_bucket(self.minio, job_id, working_directory)
                shutil.rmtree(working_directory)

                if isinstance(message, DoneMessage):
                    self.minio.put_object(job_id, 'done', BytesIO(b''), 0)
                    logging.info('Job `%s` from sheep `%s` done', job_id, sheep_id)
                elif isinstance(message, ErrorMessage):
                    error = (message.short_error + '\n' + message.long_error).encode()
                    self.minio.put_object(job_id, 'error', BytesIO(error), len(error))
                    logging.info('Job `%s` from sheep `%s` failed (%s)', job_id, sheep_id, message.short_error)

                self[sheep_id].jobs_meta.pop(job_id)
                sheep.in_progress.remove(job_id)
                self.notifier.notify()

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
        for sheep_id in self.sheep.keys():
            self.slaughter_sheep(sheep_id)

    def is_job_done(self, job_id: str) -> bool:
        if minio_object_exists(self.minio, job_id, 'done') or minio_object_exists(self.minio, job_id, 'error'):
            return True
        else:
            for sheep in self.sheep.values():
                if job_id in sheep.jobs_meta.keys():
                    return False
            else:
                raise UnknownJobError('Job `{}` is not know to this worker'.format(job_id))
