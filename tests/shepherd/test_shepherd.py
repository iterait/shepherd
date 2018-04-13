import pytest


from cxworker.sheep import BareSheep, DockerSheep, SheepConfigurationError
from cxworker.shepherd import Shepherd
from cxworker.api.errors import UnknownSheepError
from cxworker.shepherd.config import WorkerConfig


def test_shepherd_init(valid_config: WorkerConfig, minio):
    shepherd = Shepherd(valid_config.sheep, valid_config.data_root, minio, valid_config.registry)

    assert isinstance(shepherd['bare_sheep'], BareSheep)

    with pytest.raises(UnknownSheepError):
        _ = shepherd['UnknownSheep']
    shepherd.notifier.close()

    valid_config.sheep['docker_sheep'] = {'port': 9002, 'type': 'docker'}
    shepherd = Shepherd(valid_config.sheep, valid_config.data_root, minio, valid_config.registry)
    assert isinstance(shepherd['docker_sheep'], DockerSheep)
    shepherd.notifier.close()

    with pytest.raises(SheepConfigurationError):
        shepherd = Shepherd(valid_config.sheep, valid_config.data_root, minio)  # missing docker registry

    shepherd.notifier.close()

    valid_config.sheep['my_sheep'] = {'type': 'unknown'}
    with pytest.raises(SheepConfigurationError):
        Shepherd(valid_config.sheep, valid_config.data_root, minio, valid_config.registry)  # unknown sheep type


def test_shepherd_status(shepherd):
    sheep_name, sheep = next(shepherd.get_status())
    assert not sheep.running
    assert sheep_name == 'bare_sheep'
