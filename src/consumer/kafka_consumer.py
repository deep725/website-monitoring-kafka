import logging
import json

from confluent_kafka import Consumer


class KafkaConsumer:
    def __init__(self, config, db_sink):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.kafka_topic = config.kafka_topic
        self.create_consumer(config)

        self.__db_sink = db_sink

    def create_consumer(self, config):
        conf_broker = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
            'group.id': config.kafka_consumer_group,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest',
        }

        if config.kafka_sec_prot != "None":
            conf_sec = {
                'security.protocol': config.kafka_sec_prot,
                'ssl.ca.location': config.kafka_cafile,
                'ssl.certificate.location': config.kafka_certfile,
                'ssl.key.location': config.kafka_keyfile
            }
            conf_broker = {**conf_broker, **conf_sec}

            self.__consumer  = Consumer(**conf_broker) 
            self.__consumer .subscribe([self.kafka_topic])

    def get_consumer(self):
        return self.__consumer 

    def stop(self):
        self.logger.debug("Stoping Consumer...")
        self.__consumer.close()

    async def start(self):
        self.logger.debug(f"Consuming on : {self.kafka_topic}")
        while True:
            try:
                await self.poll_msgs()
            except KeyboardInterrupt:
                break

    async def poll_msgs(self):
        msg = self.__consumer.poll(1.0)
        await self.process_msg(msg)

    async def process_msg(self, msg):
        if msg is None:
            return None
        if msg.error():
            val = msg.error()
            self.logger.info(f'Consumer error: {msg.error()}')
            return None

        str = msg.value().decode("utf-8")
        data = json.loads(msg.value().decode("utf-8"))
        self.logger.info(f'Received message: {data}')
        await self.__db_sink.insert(data)
        return data



