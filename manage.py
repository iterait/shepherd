from flask_script import Manager, Shell

from CXWorker.worker import Worker

worker = Worker()
manager = Manager(worker.app, with_default_commands=False)
manager.add_command("shell", Shell())


@manager.option("-h", "--host", dest="host", default="", help="The host name to which the HTTP API should bind")
@manager.option("-p", "--port", dest="port", default=5000, help="The port to which the HTTP API should bind")
@manager.option("-c", "--config", dest="config_file", default="cxworker.yml", help="Path to a configuration file")
def run_worker(host, port, config_file):
    with open(config_file, "r") as config:
        worker.load_config(config)

    worker.run(host, port)

manager.run()