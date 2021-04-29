import json

import pytest
from unittest import mock
from unittest.mock import AsyncMock

from src.consumer.kafka_consumer import KafkaConsumer


# @pytest.fixture
# @mock.patch('src.consumer.pgsql_sink.PgSQLSink', autospec=True)
# async def kafka_consumer_obj(db_mock, kafka_consumer_mock, cfg_read, mocker):
#     return kafka_consumer_mock, KafkaConsumer(cfg_read, db_mock)

@pytest.fixture
@mock.patch('src.consumer.kafka_consumer.Consumer', autospec=True)
async def kafka_consumer_obj(kafka_consumer_mock, cfg_read):
    db_sink = AsyncMock()
    consumer_obj = KafkaConsumer(cfg_read, db_sink)
    return kafka_consumer_mock, consumer_obj


class TestKafkaConsumerObject:
    def test_consumer_create_obj(self, cfg_read, mocker):
        db_sink = mocker.MagicMock()
        with mock.patch('src.consumer.kafka_consumer.Consumer', autospec=True) as kafka_consumer_mock:
            consumer_obj = KafkaConsumer(cfg_read, db_sink)

            assert kafka_consumer_mock.return_value == consumer_obj.get_consumer()

            kafka_consumer_mock.assert_called_once()
            kafka_consumer_mock.return_value.subscribe.assert_called_once_with(
                [cfg_read.kafka_topic])

    @pytest.mark.skip(reason="Types defined in C cannot be patched. So Consumer.poll cannot be patched.")
    @pytest.mark.asyncio
    async def test_consumer_poll_msgs(self, kafka_consumer_obj, mocker):
        consumer, obj = kafka_consumer_obj
        await obj.poll_msgs()

        consumer.poll.assert_called_once()

    @pytest.mark.asyncio
    async def test_consumer_process_No_msgs(self, kafka_consumer_obj, mocker):
        _, obj = kafka_consumer_obj
        assert await obj.process_msg(None) is None

    @pytest.mark.asyncio
    async def test_consumer_process_invalid_msg(self, kafka_consumer_obj, mocker):
        _, obj = kafka_consumer_obj
        # Sending any random number as error
        msg_mock = mocker.MagicMock()
        msg_mock.error.return_value = 343
        assert await obj.process_msg(msg_mock) is None

    @pytest.mark.asyncio
    async def test_consumer_process_valid_msg(self, kafka_consumer_obj, mocker):
        _, obj = kafka_consumer_obj
        returned_msg = '{"url": "http://google.com/cc", "err_status": "404", "time": "47.0", "text_found": "yes"}'

        msg_mock = mocker.MagicMock()
        msg_mock.error.return_value = 0  # No error. Bypass to check msg.error()
        msg_mock.value.return_value = returned_msg.encode('utf-8')

        assert json.loads(returned_msg) == await obj.process_msg(msg_mock)

    def test_consumer_process_stop(self, kafka_consumer_obj):
        consumer, obj = kafka_consumer_obj
        obj.stop()

        consumer.return_value.close.assert_called_once()
