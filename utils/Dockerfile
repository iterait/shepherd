FROM python:3.6.2-stretch

WORKDIR app

COPY runner.py .

RUN apt-get update && apt-get install --yes libzmq5 && pip install pyzmq

EXPOSE 9999

CMD python3 runner.py
