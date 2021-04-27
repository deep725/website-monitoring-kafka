import pytest
from unittest import mock
from unittest.mock import Mock
from main.publisher.web_monitor_app import WebMonitorApp
from unittest.mock import patch
import main
from main.publisher.kafka_publisher import KafkaPublisher
from confluent_kafka import Producer

# @mock.patch('main.publisher.kafka_publisher.KafkaPublisher', autospec=True)
def test_web_monitor_app(cfg_read, mocker):
    loop = mocker.MagicMock()
    pr = mocker.MagicMock()
    pr.return_value = 'foo'
    # conn = mocker.patch('main.publisher.kafka_publisher.KafkaPublisher')
    # conn.return_value = 1
    
    #mocker.patch.object(main.publisher.kafka_publisher, 'KafkaPublisher', lambda:2)
    #mocker.patch.object(Producer, '', lambda:2)
   # kafkaPublisher = Mock();
   # kafkaPublisher.return_value = 1
  #  KafkaPublisher.return_value = 'foo'
    #with patch('Producer'):
    app = WebMonitorApp(cfg_read, loop)
    ret = app.get_publisher()
    breakpoint()      
    print(ret)

# class TestWebMonitorApp:
#     def test_app(self, web_monitor_app):
#         app = web_monitor_app
        # assert app.__config.get_log_level == 10
