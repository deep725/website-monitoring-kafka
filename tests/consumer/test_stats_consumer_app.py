import pytest
from unittest import mock

from src.consumer.stats_consumer_app import StatsConsumerApp


@pytest.fixture
@mock.patch('src.consumer.stats_consumer_app.KafkaConsumer', autospec=True)
@mock.patch('src.consumer.pgsql_sink.PgSQLSink', autospec=True)
def stats_consumer_app(db_mock, consumer_mock, cfg_read, mocker):
    return consumer_mock, StatsConsumerApp(cfg_read, db_mock)


class TestStatsConsumerApp:
    @pytest.mark.asyncio
    async def test_consumer_stop(self, stats_consumer_app, mocker):
        consumer, app = stats_consumer_app

        app.stop()
        consumer.return_value.stop.assert_called_once()
        
    @pytest.mark.asyncio
    async def test_consumer_start(self, stats_consumer_app, mocker):
        consumer, app = stats_consumer_app

        await app.run()
        consumer.return_value.start.assert_called_once_with()
