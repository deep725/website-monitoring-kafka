import logging
import json

from confluent_kafka import Producer
from functools import wraps
from os import sys


class KafkaPublisher:
    def __init__(self, config = None):
        self.logger = logging.getLogger(self.__class__.__name__)

        conf = self.get_broker_conf(config)
        self.__producer = Producer(**conf)
        self.kafka_topic = config.kafka_topic

    def get_broker_conf(self, config):
        conf_broker = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
        }

        conf_sec = {
            'security.protocol': config.kafka_sec_prot,
            'ssl.ca.location': config.kafka_cafile,
            'ssl.certificate.location': config.kafka_certfile,
            'ssl.key.location': config.kafka_keyfile
        }

        if config.kafka_sec_prot is not None:
            return ( {**conf_broker, **conf_sec})


    def stop(self):
        sys.stderr.write('%% Waiting for %d deliveries\n' %
                         len(self.__producer))
        self.logger.debug("Stoping Publisher...")
        self.__producer.flush()

    def delivery_callback(self, err, msg):
        if err:
            self.logger.error(f'Message delivery failed: {err}')
        else:
            self.logger.info(
                f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

    def send(self, data):
        # self = cls()
        logging.debug(f"Sending: {self.kafka_topic}")
        # self.__producer.produce(self.kafka_topic, "hj", callback=self.delivery_callback)
        self.__producer.produce(
            self.kafka_topic,
            key=data['url'],
            value=json.dumps(data).encode('utf-8'),
            callback=self.delivery_callback
        )
        self.__producer.poll(0)

    @classmethod
    def publishit(cls, func):
        @wraps(func)
        async def publisher_wrapper(*args, **kwargs):
            ret_data = await func(*args, **kwargs)

            cls.send(ret_data)
        return publisher_wrapper
