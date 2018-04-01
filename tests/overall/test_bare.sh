#!/usr/bin/env bash
set -e
# set-up
rm -rf /tmp/minio
mkdir /tmp/minio
export MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
export MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
minio server --address 0.0.0.0:7000 /tmp/minio/data &
minio_pid=$!
cxworker run -c examples/configs/cxworker-bare.yml &
worker_pid=$!

sleep 2
# test
python tests/overall/one_shot_test.py

# tear-down
kill $worker_pid
kill $minio_pid
