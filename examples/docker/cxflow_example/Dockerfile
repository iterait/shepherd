FROM cognexa/cxflow:latest

RUN pacman -Sy --noconfirm --needed python-pyzmq openssh python-numpy && paccache -rfk0

ADD ssh /root/.ssh
RUN chmod 600 -R /root/.ssh
RUN ssh-keyscan -H github.com >> /root/.ssh/known_hosts

RUN pip install git+ssh://git@github.com/cognexa/cxworker.git@dir

ENV LD_LIBRARY_PATH /usr/local/lib

EXPOSE 9999

WORKDIR /project

ADD . .

# Replace this with your model of choice
ADD cxflow-test/latest/config.yaml /project/config.yaml

CMD cxworker-runner /project
