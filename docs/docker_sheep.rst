Docker Sheep
============

:py:class:`cxworker.sheep.DockerSheep` does exactly the same as `bare sheep <bare_sheep.html>`_ except it
runs the *runner* in a docker. Similarly to the bare sheep's runner, docker runner is expected to listen for
incoming jobs at a socket bind to port **9999**.

Configuration
*************

Docker sheep's config is even simpler than in the bare sheep case:

.. code-block:: yaml

  cpu_sheep_1:
    port: 9001
    type: docker
    devices:
      - /dev/nvidia0

This simple configuration even enables GPU for your container if you have properly installed nvidia docker 2.

Model Name and Version
**********************

When new model name and version is encountered, docker sheep pulls the docker image from the configured docker registry.

Example Dockerfile follows:

.. include:: ../examples/docker/cxflow_example/Dockerfile
   :literal:
