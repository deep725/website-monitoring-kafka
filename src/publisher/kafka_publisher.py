import logging
import json

from confluent_kafka import Producer
from functools import wraps
from os import sys

class KafkaPublisher:
    def __init__(self, config):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__producer = self.get_producer(config)
        self.kafka_topic = config.kafka_topic

    def get_producer(self, config):
        conf_broker = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
        }

        if config.kafka_sec_prot != "None":
            conf_sec = {
                'security.protocol': config.kafka_sec_prot,
                'ssl.ca.location': config.kafka_cafile,
                'ssl.certificate.location': config.kafka_certfile,
                'ssl.key.location': config.kafka_keyfile
            }
            conf_broker = {**conf_broker, **conf_sec}

        return Producer(**conf_broker) 

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

