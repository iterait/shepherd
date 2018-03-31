#!/bin/sh
docker build --no-cache . -t 0.0.0.0:6000/cxflow-test:latest
docker push 0.0.0.0:6000/cxflow-test:latest
