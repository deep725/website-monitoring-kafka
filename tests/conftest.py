import pytest
from main.utils import ConfigReader
from main.publisher import WebMonitorApp

@pytest.fixture(scope="session")
def cfg_read():
    return ConfigReader('tests/config/config.test.json')

# @pytest.fixture
# @pytest.mock.patch('publisher.kafka_publisher.KafkaPublisher')
# def web_monitor_app(cfg_read, mocker):
#     loop = mocker.MagicMock()
#     KafkaPublisher.return_value = 'foo'
#     return WebMonitorApp(cfg_read, loop)
