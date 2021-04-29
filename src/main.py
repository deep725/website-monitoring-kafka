import asyncio
import logging
import multiprocessing

from publisher import WebMonitorApp
from consumer import StatsConsumerApp
from consumer import PgSQLSink
from utils import ConfigReader

# all_groups = asyncio.gather(*tasks, return_exceptions=True)
# results = loop.run_until_complete(all_groups)
# print(results)

class GracefulExit:
    def __init__(self, app, proc_name, log_file, log_level, loop=None):
        self.__loop = loop
        self.__app = app
        self.__proc_name = proc_name

        logging.basicConfig(level=log_level,
                            filename=log_file,
                            format='%(asctime)s [%(module)s::%(name)s:%(funcName)s() ] - %(message)s',
                            filemode='w')

    def __enter__(self):
        print(f'Started {self.__proc_name}')

    def __exit__(self, type, value, traceback):
        self.__app.stop()
        if self.__loop:
            self.__loop.stop()

        print(f'{self.__proc_name} finished')


def web_monitor_proc(config):
    loop = asyncio.get_event_loop()
    app = WebMonitorApp(config, loop)

    with GracefulExit(app, "web_monitor_proc", "logs/producer.log", config.log_level):
        try:
            loop.run_until_complete(app.run())
        except KeyboardInterrupt:
            pass


def stats_consumer_proc(config):
    loop = asyncio.get_event_loop()
    db_sink = PgSQLSink(config)
    app = StatsConsumerApp(config, db_sink)

    with GracefulExit(app, "stats_consumer_proc", "logs/consumer.log", config.log_level):
        try:
            loop.run_until_complete(app.run())

        except KeyboardInterrupt:
            pass

    db_sink.clean_up()


def run_procs(procs):
    try:
        procs_list = []

        for proc in procs:
            process = multiprocessing.Process(target=proc,
                                              args=(config,))
            procs_list.append(process)
            process.start()

        for proc in procs_list:
            proc.join()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    procs = (web_monitor_proc, stats_consumer_proc)

    config = ConfigReader('config/config.json')
    # config = ConfigReader('config/config.local.json')

    # ToDo: SDSINGH : Remove it
    # Create as much topic as urls list so that each url can go into its own list
    # from kafka_admin import KafkaAdmin
    # kafka_admin = KafkaAdmin()
    # kafka_admin.create_partitions( len(config.url_list), config.kafka_topic)

    run_procs(procs)
