Introduction
############

Before diving into **shepherd**, please make you have installed and configured

- **cxflow** framework
- **docker** (and nvidia docker 2 if you plan on accelerating your computations with GPUs)
- **docker registry** and **docker-compose**
- **minio** cloud storage

Note that with docker installed, most of the other dependencies can be run within it.

We provide a docker-compose command to setup local minio storage and docker registry

The docker-compose file can be found in the **shepherd** repository, so lets take a look how you can obtain it.

Installation
************
The best way to start working with **shepherd** is to clone our github repository and use pip to install it:

.. code-block:: shell

    git clone git@github.com:iterait/shepherd.git
    cd shepherd
    pip install .

Now, running the **shepherd** is incredibly easy.


.. code-block:: shell

    docker-compose -f examples/docker/docker-compose-sandbox.yml up -d  # start up minio and docker registry
    shepherd -c examples/configs/shepherd-bare.yml  # start up shepherd

That is it! You should see some output similar to this:


.. code-block:: log

    2018-04-01 00:44:23.000483: INFO    @shepherd       : Created sheep `bare_sheep` of type `bare`
    2018-04-01 00:44:23.000490: INFO    @shepherd       : Minio storage appears to be up and running
    2018-04-01 00:44:23.000490: INFO    @shepherd       : Shepherd API is available at http://0.0.0.0:5000

Basics
******

With **shepherd** up and running, you can start computing your resource intensive *jobs* with it.
An example of a job may be an inference of neural network which may take a while so better do it asynchronously, right?

In essence, a job computation consists of following steps

1. Prepare job inputs in minio bucket ``<job_id>/inputs/``
2. Call **shepherd** ``/start-job`` end-point
3. Wait for the job to be computed
4. Find your results in minio bucket ``<job_id>/outputs/``

**Shepherd** naturally tells you if your job is ready or not. Just ask him at ``/jobs/<job_id>/ready`` end-point.

The initial ``/start-job`` end-point type is POST and a JSON similar to the following one is expected:

.. code-block:: json

    {
        "job_id": "request_id",
        "model":
            {
                "name": "cxflow-test",
                "version": "latest"
            }
    }

Hopefully, you will find the contents self-explanatory.
Detailed **shepherd** API is provided here (TODO).

Behind the scenes
*****************

So by who, when and how is your job computed? Well, **shepherd** delegates the actual work to his shepherd and sheep
(poor things, right?).
A sheep will configure itself to the desired model and version so that it can compute your jobs.
A shepherd can hold multiple sheep, nonetheless, you need to address the jobs to them manually.

The important thing here is that **arbitrary number of jobs may be submitted to the shepherd and his sheep will deal with
them eventually**.

Every sheep manages its own *runner* (`more on runners <runners.html>`_) and communicates with it via sockets. So in the
end, *runners* do the heavy lifting.

Configuration
*************

You may have noticed that in the command above that the shepherd was configured with ``examples/configs/shepherd-bare.yml``.
Let's see what is in there:

.. code-block:: yaml

    data_root: /tmp/shepherd-data
    registry:
      url: http://0.0.0.0:6000
    storage:
      url: http://0.0.0.0:7000
      access_key: AKIAIOSFODNN7EXAMPLE
      secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

    sheep:
      bare_sheep:
        port: 9001
        type: bare
        working_directory: examples/docker/cxflow_example
        stdout_file: /tmp/bare-shepherd-runner-stdout.txt
        stderr_file: /tmp/bare-shepherd-runner-stderr.txt

You need to configure the minio `storage` and docker `registry` in their respective sections,
that should not surprise you.
Aside from that, **shepherd** needs a single directory to work with.
It is just fine to have it under ``/tmp`` as **shepherd** saves everything worth saving to the storage.
In the case it crashes or is restarted, this directory is cleaned-up anyways.

Finally, we can configure the sheep the **shepherd** has under its command.
At the moment, we recognize ``bare`` and ``docker`` sheep.
You can find more on how to configure them in their respective sections.

Further reading
***************

Now feel free to read about our sheep (`bare <bare_sheep.html>`_ and `docker <bare_sheep.html>`_) or how sheep actually
`run the jobs <runners.html>`_.
