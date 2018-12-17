import re
import os
import logging
import traceback
import os.path as path
from abc import abstractmethod
from typing import Optional, Any, Dict

import zmq.green as zmq

import emloop as el
from emloop.cli.common import create_dataset, create_model
from emloop.cli.util import validate_config, find_config
from emloop.utils import load_config
from shepherd.comm import *
from shepherd.constants import INPUT_DIR, OUTPUT_DIR


def n_available_gpus() -> int:
    """
    Return the number of NVIDIA GPU devices available to this process.

    .. note::
        This method attempts to parse (in-order) ``CUDA_VISIBLE_DEVICES`` env variable (bare sheep) and
        ``NVIDIA_VISIBLE_DEVICES`` env variable (docker sheep).
        If none of these variables is available, it lists GPU devices in ``/dev``.

    :return: the number of available GPU devices
    """
    if 'CUDA_VISIBLE_DEVICES' in os.environ:
        devices = os.environ['CUDA_VISIBLE_DEVICES']
    elif 'NVIDIA_VISIBLE_DEVICES' in os.environ and not os.environ['NVIDIA_VISIBLE_DEVICES'].strip() == 'all':
        devices = os.environ['NVIDIA_VISIBLE_DEVICES']
    else:
        devices = ','.join(filter(lambda d: re.search(r'nvidia[0-9]+', d) is not None, os.listdir('/dev')))

    return len(devices.split(',')) if len(devices) > 0 else 0


class BaseRunner:
    """
    Base **emloop** runner class suitable for inheritance when implementing a runner with custom behavior.
    :py:class:`BaseRunner` manages the socket, messages and many more. See :py:meth:`_process_job` for more info.
    """

    def __init__(self, config_path: str, port: int, stream_name: str):
        """Create new :py:class:`Runner`."""
        logging.info('Creating emloop runner from `%s` listening on port %s', config_path, port)

        # bind to the socket
        self._port = port
        self._socket = None

        self._config_path: str = config_path
        self._stream_name: str = stream_name
        self._config: Dict[str, Any] = None
        self._dataset: Optional[el.AbstractDataset] = None
        self._model: Optional[el.AbstractModel] = None

    def _load_config(self) -> None:
        """
        Maybe load the **emloop** configuration from previously specified file and apply updates
        from ``eval.<stream_name>`` section.
        """
        if self._config is None:
            logging.debug('Loading config from `%s', self._config_path)
            # load config
            self._config = load_config(config_file=find_config(self._config_path))
            if 'eval' in self._config and self._stream_name in self._config['eval']:
                logging.debug('Applying eval config updates for stream `%s`', self._stream_name)
                update_section = self._config['eval'][self._stream_name]
                for subsection in ['dataset', 'model', 'main_loop']:
                    if subsection in update_section:
                        self._config[subsection].update(update_section[subsection])
                if 'hooks' in update_section:
                    self._config['hooks'] = update_section['hooks']
                else:
                    logging.warning('Config does not contain `eval.%s.hooks` section. '
                                    'No hook will be employed during the evaluation.', self._stream_name)
                    self._config['hooks'] = []
            self._config["model"]["n_gpus"] = n_available_gpus()
            validate_config(self._config)
            logging.debug('Loaded config: %s', self._config)

    def _load_dataset(self) -> None:
        """Maybe load dataset."""
        if self._dataset is None:
            self._load_config()
            logging.info('Creating dataset')
            self._dataset = create_dataset(self._config, None)

    def _load_model(self) -> None:
        """Maybe load model."""
        if self._model is None:
            self._load_config()
            logging.info('Creating model')
            restore_from = self._config_path
            if not path.isdir(restore_from):
                restore_from = path.dirname(restore_from)
            self._model = create_model(self._config, None, self._dataset, restore_from)

    @abstractmethod
    def _process_job(self, input_path: str, output_path: str) -> None:
        """
        Process a job with having inputs in the ``input_path`` and save the outputs to the ``output_path``.

        :param input_path: input directory path
        :param output_path: output directory path
        """

    def process_all(self) -> None:
        """Listen on the ``self._socket`` and process the incoming jobs in an endless loop."""
        logging.info('Starting the loop')
        try:
            logging.debug('Creating socket')
            self._socket: zmq.Socket = zmq.Context().instance().socket(zmq.ROUTER)
            self._socket.setsockopt(zmq.IDENTITY, b"runner")
            self._socket.bind("tcp://0.0.0.0:{}".format(self._port))
            while True:
                logging.info('Waiting for a job')
                input_message: InputMessage = Messenger.recv(self._socket, [InputMessage])
                job_id = input_message.job_id
                io_data_root = input_message.io_data_root
                logging.info('Received job `%s` with io data root `%s`', job_id, io_data_root)
                try:
                    input_path = path.join(io_data_root, job_id, INPUT_DIR)
                    output_path = path.join(io_data_root, job_id, OUTPUT_DIR)
                    self._process_job(input_path, output_path)
                    logging.info('Job `%s` done, sending DoneMessage', job_id)
                    Messenger.send(self._socket, DoneMessage(dict(job_id=job_id)), input_message)

                except BaseException as e:
                    logging.exception(e)

                    logging.error('Sending ErrorMessage for job `%s`', job_id)
                    short_erorr = "{}: {}".format(type(e).__name__, str(e))
                    long_error = str(traceback.format_tb(e.__traceback__))
                    error_message = ErrorMessage(dict(job_id=job_id, short_error=short_erorr, long_error=long_error))
                    Messenger.send(self._socket, error_message, input_message)
        finally:
            if self._socket is not None:
                self._socket.close(0)
