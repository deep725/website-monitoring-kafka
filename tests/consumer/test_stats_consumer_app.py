import pytest
from unittest.mock import AsyncMock
from confluent_kafka import Consumer

from main.consumer.stats_consumer_app import StatsConsumerApp
from main.consumer.kafka_consumer import KafkaConsumer
 
@pytest.fixture
def stats_consumer_app(cfg_read, mocker):

    db_sink = mocker.MagicMock()
    consumer = mocker.MagicMock()
    mocker.patch.object(KafkaConsumer, "create_consumer", side_effect= consumer)

    return consumer, StatsConsumerApp(cfg_read, db_sink)

class TestStatsConsumerApp:
    @pytest.mark.asyncio 
    async def test_consumer_start(self, stats_consumer_app, mocker):
        consumer, app = stats_consumer_app

        consumer.consumer_start = AsyncMock(name="start", return_value=None)
        mocker.patch.object(KafkaConsumer, "start", side_effect= consumer.consumer_start)
        
        await app.run()
        consumer.consumer_start.assert_called_once()

    @pytest.mark.asyncio 
    async def test_consumer_stop(self, stats_consumer_app, mocker):
        consumer, app = stats_consumer_app

        consumer.consumer_stop = mocker.MagicMock(name="stop", return_value=None)
        mocker.patch.object(KafkaConsumer, "stop", side_effect= consumer.consumer_stop)
        app.stop()
        consumer.consumer_stop.assert_called_once()
