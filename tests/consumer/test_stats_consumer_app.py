import pytest
from unittest import mock

from src.consumer.stats_consumer_app import StatsConsumerApp


@pytest.fixture
@mock.patch('src.consumer.stats_consumer_app.KafkaConsumer', autospec=True)
def stats_consumer_app(consumer_mock, cfg_read, mocker):
    db_sink = mocker.MagicMock()
    return consumer_mock, StatsConsumerApp(cfg_read, db_sink)


class TestStatsConsumerApp:
    @pytest.mark.asyncio
    async def test_consumer_start(self, stats_consumer_app, mocker):
        consumer, app = stats_consumer_app

        await app.run()
        consumer.return_value.start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_consumer_stop(self, stats_consumer_app, mocker):
        consumer, app = stats_consumer_app

        app.stop()
        consumer.return_value.stop.assert_called_once()
