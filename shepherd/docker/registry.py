import requests
from requests.auth import HTTPBasicAuth

from shepherd.shepherd.config import RegistryConfig


def list_images_in_registry(config: RegistryConfig):
    response = requests.get(config.url + "/v2/_catalog", auth=HTTPBasicAuth(config.username, config.password))
    return response.json()["repositories"]
