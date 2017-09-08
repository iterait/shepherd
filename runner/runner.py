from argparse import ArgumentParser
from collections import defaultdict
import logging
import zmq

from cxflow.cli.common import create_dataset, create_model
from cxflow.cli.util import validate_config, find_config
from cxflow.utils import load_config


def jsonify(data: dict):
    """JSONify a dict of jsonifable objects (dict, list, numpy array)."""

    json_data = dict()
    for key, value in data.items():
        if isinstance(value, list): # for lists
            value = [jsonify(item) if isinstance(item, dict) else item for item in value]
        elif isinstance(value, dict): # for nested lists
            value = jsonify(value)
        elif isinstance(key, int): # if key is integer: > to string
            key = str(key)
        elif type(value).__module__ == 'numpy': # if value is numpy.*: > to python list
            value = value.tolist()
        else:
            raise ValueError('Unsupported value type: `{}`'.format(type(value)))
        json_data[key] = value
    return json_data


def runner():
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

        logging.info('Running the model')
        result_batches = [model.run(batch, train=False) for batch in dataset.predict_stream(payload)]

        logging.info('Processing the results')
        result = defaultdict(list)
        for res_batch in result_batches:
            for key, val in res_batch.items():
                result[key].append(val)

        result_json = jsonify(result)

        logging.info('Sending result')
        socket.send_multipart([identity, result_json])
