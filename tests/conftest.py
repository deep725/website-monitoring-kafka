import pytest
from src.utils import ConfigReader


@pytest.fixture(scope="session")
def cfg_read():
    return ConfigReader('tests/config/config.test.json')
