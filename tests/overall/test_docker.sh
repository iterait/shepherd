#!/usr/bin/env bash
set -e
# set-up
docker-compose -f examples/docker/docker-compose-sandbox.yml up -d
cd examples/docker/cxflow_example && ./build.sh
cd ../../../
python manage.py run_worker -h 0.0.0.0 -p 5000 -c examples/configs/cxworker-docker-cpu.yml &
worker_pid=$!

# test
python tests/overall/one_shot_test.py

# tear-down
kill $worker_pid
docker-compose -f examples/docker/docker-compose-sandbox.yml stop
