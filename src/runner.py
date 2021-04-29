import asyncio
import logging
import multiprocessing
print("In module products __package__, __name__ ==", __package__, __name__)

from .publisher import WebMonitorApp
from .consumer import StatsConsumerApp, PgSQLSink
from .utils import ConfigReader

class Runner:
    """
    A class used to run the website monitoring app. This class will 
    spawn two different processes:

    * website_monitor_proc: to fetch the required information for the 
        configured websites, and send to kafka broker
    * stats_consumer_proc: to request the information from kafka broker
        and send to Postgre SQL database


    Attributes
    ----------
    None


    Methods
    -------
    web_monitor_proc(self, config)
        Run website monitoring process to fetch the information from
        websites, and send to kafka broker.

    stats_consumer_proc(self, config)
        Run consumer process to read the information from kafka broker
        and send that information to store inside the DB.

    run_procs(self, procs, config):
        Spawn the above processes in multiprocess manner
    """

    def web_monitor_proc(self, config):
        loop = asyncio.get_event_loop()
        app = WebMonitorApp(config, loop)

        with GracefulExit(app, "web_monitor_proc", "logs/producer.log", config.log_level, loop):
            try:
                loop.run_until_complete(app.run())
            except KeyboardInterrupt:
                pass


    def stats_consumer_proc(self, config):
        loop = asyncio.get_event_loop()
        db_sink = PgSQLSink(config)
        app = StatsConsumerApp(config, db_sink)

        with GracefulExit(app, "stats_consumer_proc", "logs/consumer.log", config.log_level, loop):
            try:
                loop.run_until_complete(app.run())

            except KeyboardInterrupt:
                pass

        db_sink.clean_up()


    def run_procs(self, procs, config):
        """Spawn the monitoring and consumer processes.

        This function will execute two parallel processes to fetch the 
        website statistics and send them to DB respectively.

        Parameters
        ----------
        procs : list, optional
            List of processes to be spawned. 

        config : ConfigReader
            information about various components such as kafka, DB, etc
        """
        
        try:
            procs_list = []
            for proc in procs:
                process = multiprocessing.Process(target=proc, args=(config,))
                procs_list.append(process)
                process.start()

            for proc in procs_list:
                proc.join()
        except KeyboardInterrupt:
            pass



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
        self.__loop.stop()

        print(f'{self.__proc_name} finished')

