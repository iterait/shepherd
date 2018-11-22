import requests
from requests.auth import HTTPBasicAuth
from typing import List

from ..config import RegistryConfig


def list_images_in_registry(config: RegistryConfig) -> List[str]:
    """
    Fetch a list of images from the specified registry (versions are not taken into account).

    :param config: The registry config
    :return a list of image names (all versions of the same image share one item)
    """

    response = requests.get(config.url + "/v2/_catalog", auth=HTTPBasicAuth(config.username, config.password))
    return response.json()["repositories"]
