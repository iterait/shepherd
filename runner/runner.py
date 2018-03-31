import re
from argparse import ArgumentParser
from collections import defaultdict
import json
import logging
import numpy as np
import os
import os.path as path
import sys

import traceback
import zmq

from cxflow.cli.common import create_dataset, create_model
from cxflow.cli.util import validate_config, find_config
from cxflow.utils import load_config
from cxworker.comm import Messenger, DoneMessage, ErrorMessage, InputMessage


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
    parser.add_argument('-p', dest="port", default=9999, type=int)
    args = parser.parse_args()

    logging.info('Starting cxflow runner from `%s` listening on port %s', args.config_path, args.port)

    # bind to the socket
    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)
    socket.setsockopt(zmq.IDENTITY, b"container")
    socket.bind("tcp://*:{}".format(args.port))

    # load config
    logging.info('Loading config')
    config_path = find_config(args.config_path)
    config = load_config(config_file=config_path, additional_args=[])
    if 'predict' in config:
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
        message: InputMessage = Messenger.recv(socket, [InputMessage])

        try:
            job_id = message.job_id
            io_path = message.io_data_root
            logging.info('Processing job `%s`', job_id)
            input_path = path.join(io_path, job_id, 'inputs', 'input.json')
            payload = json.load(open(input_path))
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
            json.dump(result_json, open(path.join(io_path, job_id, 'outputs', 'output.json'), 'w'))

            logging.info('Sending result')
            Messenger.send(socket, DoneMessage(dict(job_id=job_id)), message)
        except BaseException as e:
            logging.exception(e)
            short_erorr = "{}: {}".format(type(e).__name__, str(e))
            long_error = traceback.format_tb(e.__traceback__)
            Messenger.send(socket, ErrorMessage(dict(job_id=job_id, short_erorr=short_erorr, long_error=long_error)),
                           message)


if __name__ == '__main__':
    runner()
