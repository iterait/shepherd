import re
import os
import logging
import traceback
import os.path as path
from abc import abstractmethod
from typing import Optional, Any, Dict

import zmq.green as zmq

import cxflow as cx
from cxflow.cli.common import create_dataset, create_model
from cxflow.cli.util import validate_config, find_config
from cxflow.utils import load_config
from cxworker.comm import *
from cxworker.constants import INPUT_DIR, OUTPUT_DIR


class BaseRunner:
    """
    Base **cxflow** runner class suitable for inheritance when implementing a runner with custom behavior.
    :py:class:`BaseRunner` manages the socket, messages and many more. See :py:meth:`_process_job` for more info.
    """

    def __init__(self, config_path: str, port: int, stream_name: str):
        """Create new :py:class:`Runner`."""
        logging.info('Creating cxflow runner from `%s` listening on port %s', config_path, port)

        # bind to the socket
        self._port = port
        self._socket = None

        self._config_path: str = config_path
        self._stream_name: str = stream_name
        self._config: Dict[str, Any] = None
        self._dataset: Optional[cx.AbstractDataset] = None
        self._model: Optional[cx.AbstractModel] = None

    def _load_config(self) -> None:
        """
        Maybe load the **cxflow** configuration from previously specified file and apply updates
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
            self._config["model"]["n_gpus"] = len([s for s in os.listdir("/dev")
                                                   if re.search(r'nvidia[0-9]+', s) is not None])
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
            self._model = create_model(self._config, None, self._dataset, self._config_path)

    def _get_stream(self, *args, **kwargs) -> cx.Stream:
        """Get the prediction stream."""
        self._load_dataset()
        return getattr(self._dataset, self._stream_name+'_stream')(*args, **kwargs)

    @abstractmethod
    def _process_job(self, input_path: str, output_path: str) -> None:
        """
        Process a job with having inputs in the ``input_path`` and save the outputs to the ``output_path``.

        :param input_path: input directory path
        :param output_path: output directory path
        """

    def process_all(self) -> None:
        """Listen on the ``self._socket`` and process the incoming jobs in an endless loop."""
        self._load_config()
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
