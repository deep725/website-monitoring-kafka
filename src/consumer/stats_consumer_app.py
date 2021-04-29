import logging

from .kafka_consumer import KafkaConsumer


class StatsConsumerApp:
    def __init__(self, config, db_sink):
        self.__config = config
        self.__consumer = KafkaConsumer(config, db_sink)
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run(self):
        self.logger.debug('Starting Consumer...')
        await self.__consumer.start()

    def stop(self):
        self.__consumer.stop()
