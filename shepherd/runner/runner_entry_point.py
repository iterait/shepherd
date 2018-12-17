import asyncio
import os
import sys
import logging
import os.path as path
from argparse import ArgumentParser

import emloop as el


__all__ = ['main']


def create_argparser():
    """Create and return argument parser."""
    parser = ArgumentParser('shepherd runner')
    parser.add_argument('-p', '--port', dest="port", default=9999, type=int, help='Socket port to bind to')
    parser.add_argument('-s', '--stream', default='predict', help='Dataset stream name')
    parser.add_argument('-r', '--runner', default='shepherd.runner.JSONRunner', help='Fully qualified runner class')
    parser.add_argument('config_path', help='emloop configuration file path')
    return parser


def main() -> None:
    """
    Create a runner and list on the configured port for job ``InputMessage`` s.

    Can be invoked with installed ``shepherd-runner`` command.
    """

    # parse args
    sys.path.insert(0, os.getcwd())
    logging.basicConfig(level=logging.DEBUG,
                        format=el.constants.EL_LOG_FORMAT,
                        datefmt=el.constants.EL_LOG_DATE_FORMAT)

    args = create_argparser().parse_args()

    runner_fqn = args.runner
    config_dir = args.config_path
    if path.isfile(args.config_path):
        config_dir = path.dirname(args.config_path)

    runner_config_file = path.join(config_dir, 'runner.yaml')
    if path.exists(runner_config_file):
        logging.info('Using custom runner configuration file')
        runner_config = el.utils.load_config(path.join(runner_config_file))
        runner_fqn = runner_config['runner']['class']

    # create runner
    module, class_ = el.utils.parse_fully_qualified_name(runner_fqn)
    runner = el.utils.create_object(module, class_, args=(args.config_path, args.port, args.stream))

    # listen for input messages
    asyncio.run(runner.process_all())


if __name__ == '__main__':
    main()  # pragma: no cover
