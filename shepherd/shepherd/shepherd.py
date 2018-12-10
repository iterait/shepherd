import asyncio
import logging
import shutil
import traceback
import os.path as path
from datetime import datetime
from typing import Mapping, Generator, Tuple, Dict, Any, Optional

import zmq
import zmq.asyncio


from ..constants import OUTPUT_DIR
from ..docker import list_images_in_registry
from ..storage.minio_storage import Storage
from ..config import RegistryConfig
from ..sheep import *
from ..api.models import SheepModel, ModelModel, JobStatus, JobStatusModel, ErrorModel
from ..errors.api import UnknownSheepError, UnknownJobError
from ..errors.sheep import SheepConfigurationError, SheepError
from ..utils import create_clean_dir
from ..comm import Messenger, InputMessage, DoneMessage, ErrorMessage
from ..utils.task_queue import TaskQueue


class Shepherd:
    """
    Manages creation and access to a configured set of sheep
    """

    def __init__(self,
                 sheep_config: Mapping[str, Dict[str, Any]],
                 data_root: str,
                 storage: Storage,
                 registry_config: Optional[RegistryConfig] = None):
        """
        Create the mighty Shepherd.

        :param registry_config: optional docker registry config
        :param sheep_config: sheep config
        :param data_root: directory where the task/sheep directories will be managed
        :param storage: remote storage adapter
        """
        for config in sheep_config.values():
            if config["type"] == "docker" and registry_config is None:
                raise SheepConfigurationError("To use docker sheep, you need to configure a registry URL")

        self.job_done_condition = asyncio.Condition()

        self._registry_config = registry_config
        self._storage = storage
        self._poller = zmq.asyncio.Poller()
        self._sheep: Dict[str, BaseSheep] = {}
        self._sheep_config = sheep_config
        self._sheep_tasks = {}
        self._listener = None
        self._health_checker = None
        self._job_status: Dict[str, JobStatusModel] = {}
        self._job_status_update_queue = TaskQueue(worker_count=1)

        for sheep_id, config in sheep_config.items():
            socket = zmq.asyncio.Context.instance().socket(zmq.DEALER)
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
            self._sheep[sheep_id] = sheep
            self._poller.register(socket, zmq.POLLIN)

        self._storage_inaccessible_reported = False
        self._registry_inaccessible_reported = False

    async def start(self) -> None:
        """
        Start background tasks for the shepherd.
        """
        for sheep_id, config in self._sheep_config.items():
            self._sheep_tasks[sheep_id] = [
                asyncio.create_task(self._dequeue_and_feed_jobs(sheep_id)),
                asyncio.create_task(self._health_check(sheep_id))
            ]

        self._listener = asyncio.create_task(self._listen())
        self._health_checker = asyncio.create_task(self._shepherd_health_check())

    def _get_sheep(self, sheep_id: str) -> BaseSheep:
        """
        Get the sheep with the given ``sheep_id``.

        :param sheep_id: sheep id
        :return: sheep with the given ``sheep_id``
        :raise UnknownSheepError: if the given ``sheep_id`` is not known to this shepherd
        """
        try:
            return self._sheep[sheep_id]
        except KeyError:
            raise UnknownSheepError('Unknown sheep id `{}`'.format(sheep_id))

    def _start_sheep(self, sheep_id: str, model: str, version: str) -> None:
        """
        (Re)Start the sheep with the given ``sheep_id`` and configure it to run the specified ``model``:``version``.

        :param sheep_id: sheep id to be (re)started
        :param model: model name to be loaded
        :param version: mode version to be loaded
        """
        logging.info('Starting sheep `%s` with model `%s:%s`', sheep_id, model, version)
        self._get_sheep(sheep_id).start(model, version)

    def _slaughter_sheep(self, sheep_id: str) -> None:
        """
        Slaughter (kill) the specified sheep. In particular, its container and socket are going to be terminated,

        :param sheep_id:
        :return:
        """
        logging.info('Slaughtering sheep `%s`', sheep_id)

        self._get_sheep(sheep_id).slaughter()

    async def enqueue_job(self, job_id: str, job_meta: ModelModel, sheep_id: Optional[str] = None) -> None:
        """
        En-queue the given job for execution. If specified, use a certain sheep.

        :param job_id: job id
        :param job_meta: job meta data (model name and version)
        :param sheep_id: optional sheep id, if not specified use first sheep available
        """
        logging.info('En-queueing job `%s` for sheep `%s`', job_id, sheep_id)
        if sheep_id is None:
            sheep_id = next(iter(self._sheep.keys()))
            logging.info('Job `%s` is auto-assigned to sheep `%s`', job_id, sheep_id)

        status = JobStatusModel({"model": job_meta, "status": JobStatus.QUEUED, "enqueued_at": datetime.utcnow()})
        self._job_status[job_id] = status

        status_future = await self._job_status_update_queue.enqueue_task(self._storage.set_job_status(job_id, status))
        await self._get_sheep(sheep_id).jobs_queue.put(job_id)

        # Wait for the status update to finish before returning (this way we can be sure the job was enqueued)
        await status_future

    async def _shepherd_health_check(self) -> None:
        """
        Periodically check if the shepherd and all of its dependencies work properly (and logs warnings if they do not).
        """

        while True:
            await asyncio.sleep(1)

            if not await self._storage.is_accessible() and not self._storage_inaccessible_reported:
                logging.error("The remote storage is not accessible")
                self._storage_inaccessible_reported = True
            else:
                self._storage_inaccessible_reported = False

            if self._registry_config is not None:
                try:
                    list_images_in_registry(self._registry_config)
                    self._registry_inaccessible_reported = False
                except:
                    logging.error("The Docker registry is not accessible")
                    self._registry_inaccessible_reported = True

    async def _health_check(self, sheep_id: str) -> None:
        """
        Periodically check if the specified sheep is running and resolve its in-progress jobs if not.

        :param sheep_id: id of the sheep to be checked
        """
        while True:
            await asyncio.sleep(1)
            sheep = self._get_sheep(sheep_id)
            try:
                if not sheep.running:
                    for job_id in sheep.in_progress:
                        # clean-up the working directory
                        shutil.rmtree(path.join(self._get_sheep(sheep_id).sheep_data_root, job_id))

                        # save the error
                        error = ErrorModel({'message': 'Sheep container died without notice'})
                        logging.error('Sheep `%s` encountered error when processing job `%s`: %s',
                                      sheep_id, job_id, error.message)
                        await self._report_job_failed(job_id, error, sheep)
                    sheep.in_progress = set()

                    async with self.job_done_condition:
                        self.job_done_condition.notify_all()
            except SheepError as se:
                logging.warning('Failed to check sheep\'s health '  # pragma: no cover
                                'due to the following exception: %s', str(se))

    async def _dequeue_and_feed_jobs(self, sheep_id: str) -> None:
        """
        De-queue jobs, prepare working directories and send ``InputMessage`` to the specified sheep in an end-less
        loop.

        :param sheep_id: sheep id to be fed
        """
        while True:
            sheep = self._get_sheep(sheep_id)
            job_id = await sheep.jobs_queue.get()
            logging.info('Preparing working directory for job `%s` on `%s`', job_id, sheep_id)

            # prepare working directory
            working_directory = create_clean_dir(path.join(sheep.sheep_data_root, job_id))
            await self._storage.pull_job_data(job_id, working_directory)
            create_clean_dir(path.join(working_directory, OUTPUT_DIR))

            # update the job status
            status = self._job_status[job_id].copy()
            status.status = JobStatus.PROCESSING
            status.processing_started_at = datetime.utcnow()
            await self._job_status_update_queue.enqueue_task(self._storage.set_job_status(job_id, status))

            # (re)start the sheep if needed
            model = self._job_status[job_id].model
            if model.name != sheep.model_name or model.version != sheep.model_version or not sheep.running:
                logging.info('Job `%s` requires model `%s:%s` on `%s`', job_id, model.name, model.version, sheep_id)
                # we need to wait for the in-progress jobs which are already in the socket
                async with self.job_done_condition:
                    await self.job_done_condition.wait_for(lambda: len(sheep.in_progress) == 0)
                self._slaughter_sheep(sheep_id)
                try:
                    self._start_sheep(sheep_id, model.name, model.version)
                except SheepConfigurationError as sce:
                    error = ErrorModel({
                        'message': 'Failed to start sheep for this job ({})'.format(str(sce))
                    })

                    logging.error('Sheep `%s` encountered error when processing job `%s`: %s', sheep_id, job_id, error.message)
                    await self._report_job_failed(job_id, error, sheep)
                    continue
                except Exception as ex:
                    error = ErrorModel({
                        'message': '`{}` thrown when starting sheep `{}` for job `{}`'.format(str(ex), sheep_id, job_id),
                        'exception_type': str(type(ex)),
                        'exception_traceback': str(traceback.format_tb(ex.__traceback__))
                    })

                    await self._report_job_failed(job_id, error, sheep)
                    logging.exception("Error encountered when starting sheep `%s` for job `%s`", sheep_id, job_id)
                    continue

            # send the InputMessage to the sheep
            sheep.in_progress.add(job_id)
            logging.info('Sending InputMessage for job `%s` on `%s`', job_id, sheep_id)
            await Messenger.send(sheep.socket, InputMessage(dict(job_id=job_id, io_data_root=sheep.sheep_data_root)))

            # notify the queue that the task is done
            sheep.jobs_queue.task_done()

    async def _report_job_failed(self, job_id: str, error: ErrorModel, sheep: BaseSheep) -> None:
        """
        A job has failed - remove the local copy of its data and mark it as failed in the remote storage.
        """
        status = self._job_status.pop(job_id).copy()
        status.status = JobStatus.FAILED
        status.error_details = error
        status.finished_at = datetime.utcnow()

        async with self.job_done_condition:
            self.job_done_condition.notify_all()

        try:
            shutil.rmtree(path.join(sheep.sheep_data_root, job_id), ignore_errors=True)
            await self._job_status_update_queue.enqueue_task(self._storage.set_job_status(job_id, status))
        except Exception:
            logging.exception('Error when reporting job `%s` as failed', job_id)

    async def _listen(self) -> None:
        """
        Poll the sheep output sockets, process sheep outputs and clean-up working directories in an endless loop.
        """
        while True:
            # poll the output sockets
            result = await self._poller.poll()
            sheep_ids = (sheep_id for sheep_id, sheep in self._sheep.items() if (sheep.socket, zmq.POLLIN) in result)

            # process the sheep with pending outputs
            for sheep_id in sheep_ids:
                sheep = self._get_sheep(sheep_id)
                message = await Messenger.recv(sheep.socket, [DoneMessage, ErrorMessage], noblock=True)
                job_id = message.job_id

                # clean-up the working directory and upload the results
                working_directory = path.join(self._get_sheep(sheep_id).sheep_data_root, job_id)
                await self._storage.push_job_data(job_id, working_directory)
                shutil.rmtree(working_directory)

                # save the done/error file
                if isinstance(message, DoneMessage):
                    status = self._job_status.pop(job_id).copy()
                    status.status = JobStatus.DONE
                    status.finished_at = datetime.utcnow()
                    await self._job_status_update_queue.enqueue_task(self._storage.set_job_status(job_id, status))
                    logging.info('Job `%s` from sheep `%s` done', job_id, sheep_id)
                elif isinstance(message, ErrorMessage):
                    error = ErrorModel({
                        "message": message.message,
                        "exception_type": message.exception_type,
                        "exception_traceback": message.exception_traceback
                    })
                    await self._report_job_failed(job_id, error, sheep)
                    logging.info('Job `%s` from sheep `%s` failed (%s)', job_id, sheep_id, message.short_error)

                # notify about the finished job
                sheep.in_progress.remove(job_id)

                async with self.job_done_condition:
                    self.job_done_condition.notify_all()

    def get_status(self) -> Generator[Tuple[str, SheepModel], None, None]:
        """
        Get status information for all sheep

        :return: a generator of status information
        """
        for sheep_id, sheep in self._sheep.items():
            yield sheep_id, SheepModel({
                "running": sheep.running,
                "model": {
                    "name": sheep.model_name,
                    "version": sheep.model_version
                }
            })

    def _slaughter_all(self) -> None:
        """Slaughter all sheep."""
        for sheep_id in self._sheep.keys():
            self._slaughter_sheep(sheep_id)

    async def is_job_done(self, job_id: str) -> bool:
        """
        Check if the specified job is already done.
        The behavior of this method is undefined when `enqueue_job` is called in the middle of its execution
        (with the same job_id).

        :param job_id: id of the job to be checked
        :raise UnknownJobError: if the job is not ready nor it is known to this shepherd
        :return: job ready flag
        """
        if not await self._storage.job_data_exists(job_id):
            raise UnknownJobError()

        status = await self._storage.get_job_status(job_id)

        return status is not None and status.status in (JobStatus.DONE, JobStatus.FAILED)

    async def close(self) -> None:
        """
        Perform a clean exit by slaughtering all sheeps, stopping background tasks and waiting for status updates to be
        sent.
        """
        self._slaughter_all()
        self._listener.cancel()
        self._health_checker.cancel()

        for sheep_tasks in self._sheep_tasks.values():
            for sheep_task in sheep_tasks:
                sheep_task.cancel()

        await self._job_status_update_queue.close()
