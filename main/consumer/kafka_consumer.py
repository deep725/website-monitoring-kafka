import logging
import json

from confluent_kafka import Consumer


class KafkaConsumer:
    def __init__(self, config, db_sink):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.kafka_topic = config.kafka_topic
        conf = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
            'group.id': config.kafka_consumer_group,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest',
            'security.protocol': config.kafka_sec_prot,
            'ssl.ca.location': config.kafka_cafile,
            'ssl.certificate.location': config.kafka_certfile,
            'ssl.key.location': config.kafka_keyfile
        }

        self.__consumer = Consumer(**conf)
        self.__consumer.subscribe([self.kafka_topic])

        self.__db_sink = db_sink

    def stop(self):
        self.logger.debug("Stoping Consumer...")
        self.__consumer.close()

    async def start(self):
        self.logger.debug(f"Consuming on : {self.kafka_topic}")
        while True:
            try:
                msg = self.__consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    self.logger.info(f'Consumer error: {msg.error()}')
                    continue

                data = json.loads(msg.value().decode("utf-8"))
                self.logger.info(f'Received message: {data}')
                await self.__db_sink.insert(data)

            except KeyboardInterrupt:
                break
