import logging
import json
import enum

from os import sys
from _pytest.compat import REGEX_TYPE

log_level_info = {'logging.DEBUG': logging.DEBUG,
                  'logging.INFO': logging.INFO,
                  'logging.WARNING': logging.WARNING,
                  'logging.ERROR': logging.ERROR,
                  }

class ReturnStatus(enum.Enum):
   SUCCESS = 0
   DECODE_FAIL = 1
   FILE_NOT_FOUND = 2
   FAILURE = 3

class ConfigReader:
    def __init__(self, file):
        self.__config = {}
        self.__loglevel = None
        self.__kafka_topic = None
        self.__bootstrap_servers = None
        self.__file_read_status = self.read_file(file)
        

    def file_read_status(self):
        return self.__file_read_status

    def read_file(self, file):
        ret_val = ReturnStatus.FAILURE
        self.logger = logging.getLogger(self.__class__.__name__)
        try:
            with open(file, "r") as jsonfile:
                self.__config = json.load(jsonfile)

            self.__loglevel = log_level_info[self.__config["log_level"]]
            self.__monitoring_interval = self.__config["monitoring_interval"]
            self.__kafka_topic = self.__config["kafka"]["topic"]
            self.__bootstrap_servers = self.__config["kafka"]["bootstrap_servers"]
            self.__kafka_consumer_group = self.__config["kafka"]["consumer_group"]
            self.__kafka_sec_prot = self.__config["kafka"]["sec_prot"]
            self.__kafka_cafile = self.__config["kafka"]["cafile"]
            self.__kafka_certfile = self.__config["kafka"]["certfile"]
            self.__kafka_keyfile = self.__config["kafka"]["keyfile"]

            self.__host = self.__config["db"]["host"]
            self.__port = self.__config["db"]["port"]
            self.__database = self.__config["db"]["database"]
            self.__user = self.__config["db"]["user"]
            self.__password = self.__config["db"]["password"]
            self.__table = self.__config["db"]["table"]
            ret_val = ReturnStatus.SUCCESS

        except json.decoder.JSONDecodeError as error:
            sys.stderr.write('Exception Decoding JSON has failed: {}'.format(error))
            ret_val = ReturnStatus.DECODE_FAIL

        except FileNotFoundError as error:
            sys.stderr.write("Exception FileNotFoundError error: {}".format(error))
            ret_val = ReturnStatus.FILE_NOT_FOUND

        except Exception as error:
            sys.stderr.write("Exception thrown: {}".format(error))
            ret_val = ReturnStatus.FAILURE

        return ret_val

    @property
    def url_list(self):
        return self.__config["url_list"]

    @property
    def log_level(self):
        return self.__loglevel

    @property
    def monitoring_interval(self):
        return self.__monitoring_interval

    @log_level.setter
    def log_level(self, value):
        if value not in log_level_info.values():
            raise ValueError("Invalid log level specified")
        self.__loglevel = value

    @property
    def kafka_topic(self):
        return self.__kafka_topic

    @property
    def kafka_bootstrap_servers(self):
        return self.__bootstrap_servers

    @property
    def kafka_consumer_group(self):
        return self.__kafka_consumer_group

    @property
    def db_host(self):
        return self.__host

    @property
    def db_port(self):
        return self.__port

    @property
    def db_database(self):
        return self.__database

    @property
    def db_user(self):
        return self.__user

    @property
    def db_password(self):
        return self.__password

    @property
    def db_table(self):
        return self.__table

    @property
    def kafka_sec_prot(self):
        return self.__kafka_sec_prot

    @property
    def kafka_cafile(self):
        return self.__kafka_cafile

    @property
    def kafka_certfile(self):
        return self.__kafka_certfile

    @property
    def kafka_keyfile(self):
        return self.__kafka_keyfile
