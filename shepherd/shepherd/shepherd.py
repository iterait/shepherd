import logging
import shutil
import os.path as path
from functools import partial
from typing import Mapping, Generator, Tuple, Dict, Any, Optional

import gevent
import zmq.green as zmq
from minio import Minio

from ..constants import OUTPUT_DIR
from ..docker import list_images_in_registry
from ..storage.minio_storage import MinioStorage
from ..config import RegistryConfig
from ..sheep import *
from ..api.models import SheepModel
from ..api.models import ModelModel
from shepherd.errors.sheep import SheepConfigurationError
from ..errors.api import UnknownSheepError, UnknownJobError
from ..utils import create_clean_dir
from ..comm import Messenger, InputMessage, DoneMessage, ErrorMessage, JobDoneNotifier


class Shepherd:
    """
    Manages creation and access to a configured set of sheep
    """

    def __init__(self,
                 sheep_config: Mapping[str, Dict[str, Any]],
                 data_root: str,
                 minio: Minio,
                 registry_config: Optional[RegistryConfig]=None):
        """
        Create mighty Shepherd.

        :param registry_config: optional docker registry config
        :param sheep_config: sheep config
        :param data_root: directory where the task/sheep directories will be managed
        :param minio: Minio handle
        """
        for config in sheep_config.values():
            if config["type"] == "docker" and registry_config is None:
                raise SheepConfigurationError("To use docker sheep, you need to configure a registry URL")

        self.registry_config = registry_config
        self.storage = MinioStorage(minio)
        self.poller = zmq.Poller()
        self.sheep: Dict[str, BaseSheep] = {}
        self.notifier: JobDoneNotifier = JobDoneNotifier()

        for sheep_id, config in sheep_config.items():
            socket = zmq.Context.instance().socket(zmq.DEALER)
            sheep_type = config["type"]
            sheep_data_root = create_clean_dir(path.join(data_root, sheep_id))
            common_kwargs = {'socket': socket, 'sheep_data_root': sheep_data_root}
            if sheep_type == "docker":
                sheep = DockerSheep(config=config, registry_config=registry_config, **common_kwargs)
            elif sheep_type == "bare":
                sheep = BareSheep(config=config, **common_kwargs)
            else:
                raise SheepConfigurationError("Unknown sheep type: {}".format(sheep_type))

            logging.info('Created sheep `%s` of type `%s`', sheep_id, sheep_type)
            self.sheep[sheep_id] = sheep
            self.poller.register(socket, zmq.POLLIN)
            gevent.spawn(partial(self.dequeue_and_feed_jobs, sheep_id))
            gevent.spawn(partial(self.sheep_health_check, sheep_id))

        self._listener = gevent.spawn(self.listen)
        self._health_checker = gevent.spawn(self.shepherd_health_check)

        self.storage_inaccessible_reported = False
        self.registry_inaccessible_reported = False

    def __getitem__(self, sheep_id: str) -> BaseSheep:
        """
        Get the sheep with the given ``sheep_id``.

        :param sheep_id: sheep id
        :return: sheep with the given ``sheep_id``
        :raise UnknownSheepError: if the given ``sheep_id`` is not known to this shepherd
        """
        if sheep_id not in self.sheep:
            raise UnknownSheepError('Unknown sheep id `{}`'.format(sheep_id))
        return self.sheep[sheep_id]

    def start_sheep(self, sheep_id: str, model: str, version: str) -> None:
        """
        (Re)Start the sheep with the given ``sheep_id`` and configure it to run the specified ``model``:``version``.

        :param sheep_id: sheep id to be (re)started
        :param model: model name to be loaded
        :param version: mode version to be loaded
        """
        logging.info('Starting sheep `%s` with model `%s:%s`', sheep_id, model, version)
        self[sheep_id].start(model, version)

    def slaughter_sheep(self, sheep_id: str) -> None:
        """
        Slaughter (kill) the specified sheep. In particular, it's container and socket are going to be terminated,

        :param sheep_id:
        :return:
        """
        logging.info('Slaughtering sheep `%s`', sheep_id)
        self[sheep_id].slaughter()

    def enqueue_job(self, job_id: str, job_meta: ModelModel, sheep_id: Optional[str]=None) -> None:
        """
        En-queue the given job for execution. If specified, use a certain sheep.

        :param job_id: job id
        :param job_meta: job meta data (model name and version)
        :param sheep_id: optional sheep id, if not specified use first sheep available
        """
        logging.info('En-queueing job `%s` for sheep `%s`', job_id, sheep_id)
        if sheep_id is None:
            sheep_id = next(iter(self.sheep.keys()))
            logging.info('Job `%s` is auto-assigned to sheep `%s`', job_id, sheep_id)
        self[sheep_id].jobs_meta[job_id] = job_meta
        self[sheep_id].jobs_queue.put(job_id)

    def shepherd_health_check(self) -> None:
        while True:
            gevent.sleep(1)

            if not self.storage.is_accessible() and not self.storage_inaccessible_reported:
                logging.warning("The remote storage is not accessible")
                self.storage_inaccessible_reported = True
            else:
                self.storage_inaccessible_reported = False

            if self.registry_config is not None:
                try:
                    list_images_in_registry(self.registry_config)
                    self.registry_inaccessible_reported = False
                except:
                    logging.warning("The Docker registry is not accessible")
                    self.registry_inaccessible_reported = True

    def sheep_health_check(self, sheep_id: str) -> None:
        """
        Periodically check if the specified sheep is running and resolve its in-progress jobs if not.

        :param sheep_id: id of the sheep to be checked
        """
        while True:
            gevent.sleep(1)
            sheep = self[sheep_id]
            try:
                if not sheep.running:
                    for job_id in sheep.in_progress:
                        # clean-up the working directory
                        shutil.rmtree(path.join(self[sheep_id].sheep_data_root, job_id))

                        # save the error
                        error = 'Sheep container died without notice'
                        logging.error('Sheep `%s` encountered error when processing job `%s`: %s',
                                      sheep_id, job_id, error)
                        self.storage.job_failed(job_id, error)
                    sheep.in_progress = set()
                    self.notifier.notify()
            except SheepError as se:
                logging.warning('Failed to check sheep\'s health '  # pragma: no cover
                                'due to the following exception: %s', str(se))

    def dequeue_and_feed_jobs(self, sheep_id: str) -> None:
        """
        De-queue jobs, prepare working directories and send ``InputMessage`` to the specified sheep in an end-less
        loop.

        :param sheep_id: sheep id to be fed
        """
        while True:
            sheep = self[sheep_id]
            job_id = sheep.jobs_queue.get()
            logging.info('Preparing working directory for job `%s` on `%s`', job_id, sheep_id)

            # prepare working directory
            working_directory = create_clean_dir(path.join(sheep.sheep_data_root, job_id))
            self.storage.pull_job_data(job_id, working_directory)
            create_clean_dir(path.join(working_directory, OUTPUT_DIR))

            # (re)start the sheep if needed
            model = sheep.jobs_meta[job_id]
            if model.name != sheep.model_name or model.version != sheep.model_version or not sheep.running:
                logging.info('Job `%s` requires model `%s:%s` on `%s`', job_id, model.name, model.version, sheep_id)
                # we need to wait for the in-progress jobs which are already in the socket
                self.notifier.wait_for(lambda: len(sheep.in_progress) == 0)
                self.slaughter_sheep(sheep_id)
                try:
                    self.start_sheep(sheep_id, model.name, model.version)
                except SheepConfigurationError as sce:
                    shutil.rmtree(path.join(sheep.sheep_data_root, job_id))
                    error = 'Failed to start sheep for this job ({})'.format(str(sce))
                    logging.error('Sheep `%s` encountered error when processing job `%s`: %s', sheep_id, job_id, error)
                    self.storage.job_failed(job_id, error)
                    continue
            # send the InputMessage to the sheep
            sheep.in_progress.add(job_id)
            sheep.jobs_meta.pop(job_id)
            logging.info('Sending InputMessage for job `%s` on `%s`', job_id, sheep_id)
            Messenger.send(sheep.socket, InputMessage(dict(job_id=job_id, io_data_root=sheep.sheep_data_root)))

    def listen(self) -> None:
        """
        Poll the sheep output sockets, process sheep outputs and clean-up working directories in an end-less loop.
        """
        while True:
            # poll the output sockets
            result = self.poller.poll()
            sheep_ids = (sheep_id for sheep_id, sheep in self.sheep.items() if (sheep.socket, zmq.POLLIN) in result)

            # process the sheep with pending outputs
            for sheep_id in sheep_ids:
                sheep = self[sheep_id]
                message = Messenger.recv(sheep.socket, [DoneMessage, ErrorMessage])
                job_id = message.job_id

                # clean-up the working directory and upload the results
                working_directory = path.join(self[sheep_id].sheep_data_root, job_id)
                self.storage.push_job_data(job_id, working_directory)
                shutil.rmtree(working_directory)

                # save the done/error file
                if isinstance(message, DoneMessage):
                    self.storage.job_done(job_id)
                    logging.info('Job `%s` from sheep `%s` done', job_id, sheep_id)
                elif isinstance(message, ErrorMessage):
                    error = (message.short_error + '\n' + message.long_error)
                    self.storage.job_failed(job_id, error)
                    logging.info('Job `%s` from sheep `%s` failed (%s)', job_id, sheep_id, message.short_error)

                # notify about the finished job
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
        """Slaughter all sheep."""
        for sheep_id in self.sheep.keys():
            self.slaughter_sheep(sheep_id)

    def is_job_done(self, job_id: str) -> bool:
        """
        Check if the specified job is already done.

        :param job_id: id of the job to be checked
        :raise UnknownJobError: if the job is not ready nor it is known to this shepherd
        :return: job ready flag
        """
        if self.storage.is_job_done(job_id):
            return True

        for sheep in self.sheep.values():
            if job_id in sheep.jobs_meta.keys() or job_id in sheep.in_progress:
                return False
        else:
            raise UnknownJobError('Job `{}` is not know to this shepherd'.format(job_id))

    def close(self):
        self.slaughter_all()
        self.notifier.close()
        self._listener.kill()
        self._health_checker.kill()
