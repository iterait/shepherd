import pytest


from cxworker.docker import DockerContainer, DockerImage

docker_container_kwargs = [{},
                           {'autoremove': False},
                           {'runtime': 'nvidia'},
                           {'env': {'my_env': 'my_value'}},
                           {'bind_mounts': {'/tmp': '/tmp/host'}},
                           {'ports': {42: 84, 999: 9000}},
                           {'autoremove': False, 'runtime': 'nvidia', 'env': {'my_env': 'my_value'},
                            'bind_mounts': {'/tmp': '/tmp/host'}, 'ports': {42: 84, 999: 9000},
                            'command': 'echo'}]
docker_commands = ['run -d --rm <<IMAGE>>',
                   'run -d <<IMAGE>>',
                   'run -d --rm --runtime=nvidia <<IMAGE>>',
                   'run -d -e my_env=my_value --rm <<IMAGE>>',
                   'run -d --rm --mount type=bind,source=/tmp,target=/tmp/host <<IMAGE>>',
                   'run -d -p 0.0.0.0:42:84 -p 0.0.0.0:999:9000 --rm <<IMAGE>>',
                   'run -d -p 0.0.0.0:42:84 -p 0.0.0.0:999:9000 -e my_env=my_value --runtime=nvidia '
                   '--mount type=bind,source=/tmp,target=/tmp/host <<IMAGE>> echo']

assert len(docker_container_kwargs) == len(docker_commands)


@pytest.mark.parametrize('command,kwargs', zip(docker_commands, docker_container_kwargs))
def test_commands(command, kwargs, registry_config):
    image = DockerImage('pritunl/archlinux', 'latest', registry_config)
    command = command.replace('<<IMAGE>>', image.full_name).split(' ')
    container = DockerContainer(image=image, **kwargs)
    assert command == container._build_run_command()
