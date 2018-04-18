Runners
=======

Runners are rather small classes which only purpose is to process *jobs*, one by one.
**cxworker** provides a :py:class:`cxworker.runner.BaseRunner` to inherit your *runners* from.

All the runners may be invoked with ``cxworker-runner`` command.

cxworker-runner
---------------
.. argparse::
   :ref: cxworker.runner.runner_entry_point.create_argparser
   :prog: cxworker-runner

Custom Runners
**************

In most cases, you can just inherit from :py:class:`cxworker.runner.BaseRunner` and override
:py:meth:`cxworker.runner.BaseRunner._process_job`.

This is exactly what is done by the :py:class:`cxworker.runner.JSONRunner` class.

.. code-block:: python

    def _process_job(self, input_path: str, output_path: str) -> None:   # simplified
        payload = json.load(open(path.join(input_path, 'input')))
        result = defaultdict(list)
        for input_batch in self._get_stream(payload):
            logging.info('Another batch (%s)', list(input_batch.keys()))
            output_batch = self._model.run(input_batch, train=False, stream=None)
            if hasattr(self._dataset, 'postprocess_batch'):
                logging.info('\tPostprocessing')
                result_batch = self._dataset.postprocess_batch(input_batch=input_batch, output_batch=output_batch)
                logging.info('\tdone')
            else:
                logging.info('Skipping postprocessing')
                result_batch = output_batch

            for source, value in result_batch.items():
                result[source] += list(value)

        logging.info('JSONify')
        result_json = to_json_serializable(result)
        json.dump(result_json, open(path.join(output_path, 'output'), 'w'))

``JSONRunner`` simply loads JSON from ``inputs/input`` file, creates a stream from it and writes the output
batches to ``outputs/output``.