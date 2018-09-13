Bare Sheep
==========

:py:class:`shepherd.sheep.BareSheep` uses a sub-process to run its *runner*.
In particular, it runs ``shepherd-runner`` command which is linked to :py:func:`shepherd.runner.runner_entry_point.run`.

As mentioned earlier, a sheep communicates with its runner via a socket.
The messages are minimal though, in principle, only ``job_id`` s are passed through the socket while the data are
prepared in a folder.

Configuration
*************

Bare sheep's config is fairly simple:

.. code-block:: yaml

  bare_sheep:
    port: 9001
    type: bare
    working_directory: examples/docker/cxflow_example
    stdout_file: /tmp/bare-shepherd-runner-stdout.txt
    stderr_file: /tmp/bare-shepherd-runner-stderr.txt

Aside from configuration common for all sheep (socket ``port`` and sheep ``type``), bare sheep allows to configure:

- ``working_directory`` directory from which ``shepherd-runner`` command is called
- ``stdout_file`` and ``stderr_file`` to store the **runner** outputs

Model Name and Version
**********************

Bare sheep allows to run *jobs* with any model name and version as long as a **cxflow** configuration file can be
found in ``working_directory``/``model_name``/``model_version``/``config.yaml``.

For example with ``working_directory="/var"``, ``model_name="my_project/models"`` and finally ``model_version`` empty,
the config file is expected to be located in ``/var/my_project/models/config.yaml``.

Usage
*****

Use bare sheep whenever a docker is unavailable or your use case is simple.
In other cases, `docker sheep <docker_sheep.html>`_ is usually recommended.
