from .registry import ContainerRegistry


class OutputListener:
    """
    Waits for output from containers and stores it
    """

    def __init__(self, registry: ContainerRegistry):
        self.registry = registry

    def listen(self):
        while True:
            container_ids = self.registry.wait_for_output()

            for id in container_ids:
                output = self.registry.read_output(id)
                print('Received output:', output)
                # TODO store output, notify the dealer
