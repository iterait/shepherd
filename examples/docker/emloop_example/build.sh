#!/bin/sh
docker build --no-cache . -t 0.0.0.0:6000/emloop-test:latest
docker push 0.0.0.0:6000/emloop-test:latest
