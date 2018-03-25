# cxflow runner docker example
This directory contains simple config, dataset, model and Dockerfile with cxflow docker runner.

A container of this image may be utilized with **cxworker** for predicting new examples.

To build and test it

- create a `ssh` dir with SSH keys having access to **cxworker** GitHub
- start a local docker registry with the provided docker compose sandbox (`../docker-compose-sandbox.yml`)
- build, tag and push the image to the registry with `./build.sh`

With that, you should be able to configure the runner to pull this image and create a task for it.
