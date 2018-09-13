import pytest
import gevent


from shepherd.docker import DockerContainer, DockerImage, DockerError
from shepherd.docker.utils import run_docker_command

from .docker_not_available import docker_not_available

docker_container_kwargs = [{},
                           {'autoremove': False},
                           {'runtime': 'nvidia'},
                           {'env': {'my_env': 'my_value'}},
                           {'bind_mounts': {'/tmp': '/tmp/host'}},
                           {'ports': {42: 84, 999: 9000}},
                           {'autoremove': False, 'runtime': 'nvidia', 'env': {'my_env': 'my_value'},
                            'bind_mounts': {'/tmp': '/tmp/host'}, 'ports': {42: 84, 999: 9000},
                            'command': ['echo']}]
docker_commands = ['run -d --rm <<IMAGE>>',
                   'run -d <<IMAGE>>',
                   'run -d --rm --runtime=nvidia <<IMAGE>>',
                   'run -d -e my_env=my_value --rm <<IMAGE>>',
                   'run -d --rm --mount type=bind,source=/tmp,target=/tmp/host <<IMAGE>>',
                   'run -d -p 0.0.0.0:42:84 -p 0.0.0.0:999:9000 --rm <<IMAGE>>',
                   'run -d -p 0.0.0.0:42:84 -p 0.0.0.0:999:9000 -e my_env=my_value --runtime=nvidia '
                   '--mount type=bind,source=/tmp,target=/tmp/host <<IMAGE>> echo']

assert len(docker_container_kwargs) == len(docker_commands)


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
@pytest.mark.parametrize('command,kwargs', zip(docker_commands, docker_container_kwargs))
def test_commands(command, kwargs, registry_config, image_valid):
    image = DockerImage(*image_valid, registry_config)
    command = command.replace('<<IMAGE>>', image.full_name).split(' ')
    container = DockerContainer(image=image, **kwargs)
    assert command == container._build_run_command()


@pytest.mark.skipif(docker_not_available(), reason='Docker is not available.')
def test_docker_container(registry_config, image_valid):
    image = DockerImage(*image_valid, registry_config)
    image.pull()

    num_running_before = len(run_docker_command(['ps']).split('\n'))
    container = DockerContainer(image, command=['sleep', '10'])
    assert not container.running
    container.start()
    gevent.sleep(0.2)
    num_running_now = len(run_docker_command(['ps']).split('\n'))
    assert num_running_before + 1 == num_running_now
    assert container.running
    container.kill()
    gevent.sleep(0.2)
    num_running_final = len(run_docker_command(['ps']).split('\n'))
    assert num_running_final == num_running_before

    with pytest.raises(DockerError):
        container.kill()
