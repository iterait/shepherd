FROM iterait/emloop

RUN pacman -Syu --noconfirm --needed python-pyzmq openssh python-numpy

ADD ssh /root/.ssh
RUN chmod 600 -R /root/.ssh
RUN ssh-keyscan -H github.com >> /root/.ssh/known_hosts

RUN pip install git+ssh://git@github.com/iterait/shepherd.git

ENV LD_LIBRARY_PATH /usr/local/lib

EXPOSE 9999

WORKDIR /project

ADD . .

# Replace this with your model of choice
ADD emloop-test /project/config.yaml

CMD shepherd-runner /project
