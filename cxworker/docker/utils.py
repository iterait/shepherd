import logging
import subprocess
from typing import List

from .errors import DockerError


def kill_blocking_container(host_port: int) -> None:
    """
    List all the running docker container mapping and attempt kill any container holding the given port.

    :param host_port: host port to be freed
    """
    host_port = str(host_port)
    ps_info = run_docker_command(["ps", "--format", "{{.Ports}}\t{{.Names}}"])
    for ps_line in ps_info.split('\n'):
        if len(ps_line.strip()) == 0:
            continue
        port_mappings, name = ps_line.split('\t')
        for port_mapping in port_mappings.split(','):
            host_port_held = port_mapping.split(':')[1].split('->')[0]
            if host_port_held == host_port:
                logging.info('Killing docker container `%s` as it holds port %s', name, host_port)
                run_docker_command(['kill', name])
                return


def run_docker_command(command: List[str]) -> str:
    """
    Run and wait the given docker command. Return its stdout.

    :param command: docker command to be run as a lex list
    :raise DockerError: on failure
    :return: command stdout
    """
    command = ['docker'] + command
    plain_command = ' '.join(command)
    logging.debug('Running command `%s`', plain_command)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return_code = process.wait()
    stderr = process.stderr.read().decode()

    if len(stderr):
        logging.warning("Non-empty stderr when running command `%s`: %s", plain_command, stderr)
    if return_code != 0:
        raise DockerError('Running command `{}` failed.'.format(plain_command), return_code, stderr)

    stdout = process.stdout.read().decode()
    return stdout
