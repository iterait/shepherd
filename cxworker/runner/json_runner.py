import json
import logging
import os.path as path
from typing import Any
from collections import defaultdict

import cxflow as cx
import numpy as np

from cxworker.constants import DEFAULT_PAYLOAD_FILE, DEFAULT_OUTPUT_FILE
from .base_runner import BaseRunner


def to_json_serializable(data):
    """Make an object containing numpy arrays/scalars JSON serializable."""

    if data is None:
        return None
    if isinstance(data, dict):
        return {key: to_json_serializable(value) for key, value in data.items()}
    elif isinstance(data, list) or isinstance(data, tuple):
        return [to_json_serializable(v) for v in data]
    elif isinstance(data, np.ndarray):
        return data.tolist()
    elif np.isscalar(data):
        return data
    else:
        raise ValueError('Unsupported JSON type `{}` (key `{}`)'.format(type(data), data))


def run(model: cx.AbstractModel, dataset: cx.AbstractDataset, stream_name: str, payload: Any) -> cx.Batch:
    """
    Get the specified data stream from the given dataset, apply the given model on its batches and return the results.

    The components have to be **cxflow** compatible with:
        - dataset having method named ``[stream_name]_stream`` taking the payload and returning the stream
        - (optional) dataset having method named ``postprocess_batch`` taking both the input and output batches and
            returning the post-processed batch

    :param model: cxflow model to be run
    :param dataset: cxflow dataset to get the stream from
    :param stream_name: stream name
    :param payload: payload passed to the method creating the stream
    :return: result batch (if the stream produces multiple batches its the concatenation of all the results)
    """
    result = defaultdict(list)
    for input_batch in getattr(dataset, stream_name + '_stream')(payload):
        logging.info('Another batch (%s)', list(input_batch.keys()))
        output_batch = model.run(input_batch, train=False, stream=None)
        if hasattr(dataset, 'postprocess_batch'):
            logging.info('\tPostprocessing')
            result_batch = dataset.postprocess_batch(input_batch=input_batch, output_batch=output_batch)
            logging.info('\tdone')
        else:
            logging.info('Skipping postprocessing')
            result_batch = output_batch

        for source, value in result_batch.items():
            result[source] += list(value)
    return result


class JSONRunner(BaseRunner):
    """
    Fully functional cxflow runner which loads a JSON from ``input_path``/``input.json``, passes the loaded object
    to the desired dataset stream, runs the model and saves the output batch to ``output_path``/``output.json``.
    """

    def _process_job(self, input_path: str, output_path: str) -> None:
        """
        Process a JSON job
            - load ``input_path``/``input``
            - create dataset stream with the loaded JSON
            - run the model
            - save the output to ``output_path``/``output``

        :param input_path: input data directory
        :param output_path: output data directory
        """
        self._load_dataset()
        self._load_model()
        payload = json.load(open(path.join(input_path, DEFAULT_PAYLOAD_FILE)))
        result = run(self._model, self._dataset, self._stream_name, payload)
        result_json = to_json_serializable(result)
        json.dump(result_json, open(path.join(output_path, DEFAULT_OUTPUT_FILE), 'w'))
