from argparse import ArgumentParser
from collections import defaultdict
import json
import logging
import numpy as np
import os
import sys
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

    # load config
    logging.info('Creating dataset')
    dataset = create_dataset(config, None)

    # load model
    logging.info('Creating model')
    model = create_model(config, None, dataset, args.config_path)

    # start the loop
    logging.info('Starting the loop')
    while True:
        logging.info('Waiting for payload')
        identity, payload, *_ = socket.recv_multipart()

        payload = payload.decode()
        payload = json.loads(payload)

        logging.info('Running the model')
        result_batches = [model.run(batch, train=False) for batch in dataset.predict_stream(payload)]

        logging.info('Processing the results')
        result = defaultdict(list)
        for res_batch in result_batches:
            for key, val in res_batch.items():
                result[key] += val.tolist()

        logging.info('JSONify')
        result_json = to_json_serializable(result)
        encoded_result = json.dumps(result_json).encode()

        logging.info('Sending result')
        socket.send_multipart([identity, encoded_result])


if __name__ == '__main__':
    runner()
