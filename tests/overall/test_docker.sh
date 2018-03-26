#!/usr/bin/env bash
docker-compose -f examples/docker/docker-compose-sandbox.yml up -d
cd examples/docker/cxflow_example && ./build.sh
cd ../../../
python manage.py run_worker -h 0.0.0.0 -p 5000 -c examples/configs/cxworker-docker-cpu.yml &
python tests/overall/test_docker.py
