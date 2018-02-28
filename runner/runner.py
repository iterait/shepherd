import re
from argparse import ArgumentParser
from collections import defaultdict
import json
import logging
import numpy as np
import os
import sys

import traceback
import zmq

from cxflow.cli.common import create_dataset, create_model
from cxflow.cli.util import validate_config, find_config
from cxflow.utils import load_config


def to_json_serializable(data):
    """Make an object containing numpy arrays/scalars JSON serializable."""

    if isinstance(data, dict):
        return {key: to_json_serializable(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [to_json_serializable(v) for v in data]
    elif isinstance(data, np.ndarray):
        return data.tolist()
    elif np.isscalar(data):
        return data
    else:
        raise ValueError('Unsupported JSON type `{}` (key `{}`)'.format(type(data), data))


def send_error(socket: zmq.Socket, identity: bytes, message: bytes, additional_message: bytes = None):
    socket.send_multipart([identity, b"error", message] + ([additional_message] if additional_message is not None else []))


def runner():
    sys.path.insert(0, os.getcwd())

    # basic setup
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    parser = ArgumentParser('cxworker runner')
    parser.add_argument('config_path')
    args = parser.parse_args()

    # socket magic
    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)
    socket.setsockopt(zmq.IDENTITY, b"container")
    socket.bind("tcp://*:9999")

    # load config
    logging.info('Loading config')
    config_path = find_config(args.config_path)
    config = load_config(config_file=config_path, additional_args=[])
    assert 'predict' in config
    for section in ['dataset', 'model']:
        if section in config['predict']:
            for key in config['predict'][section]:
                config[section][key] = config['predict'][section][key]
    validate_config(config)
    logging.debug('Loaded config: %s', config)

    config["model"]["n_gpus"] = len([s for s in os.listdir("/dev") if re.search(r'nvidia[0-9]+', s) is not None])

    # create dataset
    logging.info('Creating dataset')
    dataset = create_dataset(config, None)

    # create model
    logging.info('Creating model')
    model = create_model(config, None, dataset, args.config_path)

    # start the loop
    logging.info('Starting the loop')

    while True:
        logging.info('Waiting for payload')
        identity, message_type, payload, *_ = socket.recv_multipart()

        if message_type == b"input":
            try:
                payload = payload.decode()

                logging.info('Payload accepted')
                result = defaultdict(list)
                for input_batch in dataset.predict_stream(payload):
                    logging.info('Another batch (%s)', list(input_batch.keys()))
                    output_batch = model.run(input_batch, train=False, stream=None)
                    if hasattr(dataset, 'postprocess_batch'):
                        logging.info('\tPostprocessing')
                        result_batch = dataset.postprocess_batch(input_batch=input_batch,
                                                                 output_batch=output_batch)
                        logging.info('\tdone')
                    else:
                        logging.info('Skipping postprocessing')
                        result_batch = output_batch

                    for source, value in result_batch.items():
                        result[source] += list(value)

                logging.info('JSONify')
                result_json = to_json_serializable(result)
                encoded_result = json.dumps(result_json).encode()

                logging.info('Sending result')
                socket.send_multipart([identity, b"output", encoded_result])
            except BaseException as e:
                logging.exception(e)
                send_error(socket, identity, "{}: {}".format(type(e).__name__, str(e), traceback.format_tb(e.__traceback__)).encode())
        else:
            send_error(socket, identity, b"Unknown message type received")


if __name__ == '__main__':
    runner()
