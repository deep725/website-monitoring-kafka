import psycopg2
import logging


class PgSQLSink:
    def __init__(self, config):
        self.cursor = None
        self.conn = None
        self.logger = logging.getLogger(self.__class__.__name__)

        self.table = config.db_table
        self.db_connect(config.db_database, config.db_user,
                        config.db_password, config.db_host, config.db_port)

        if self.cursor:
            self.create_table()

    def db_connect(self, database, user, password, host, port):
        try:
            self.conn = psycopg2.connect(database=database, user=user,
                                         password=password, host=host, port=port)

            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            self.logger.debug('Opened database successfully')

        except psycopg2.OperationalError as err:
            breakpoint()
            self.logger.error(f'{err}')

    def connection(self):
        return self.conn

    def create_table(self):
        """ create tables in the PostgreSQL database"""

        sql_str = f"""CREATE TABLE IF NOT EXISTS {self.table} (
          id SERIAL PRIMARY KEY,
          url VARCHAR(255) NOT NULL,
          site_status VARCHAR(20) NOT NULL,
          response_time real,
          text_found  BOOLEAN NOT NULL
          )
        """

        self.cursor.execute(sql_str)

    async def insert(self, data):
        try:
            sql_str = f"""INSERT INTO {self.table}
                    (url, site_status, response_time, text_found)
                    VALUES (%s, %s, %s, %s)"""

            data_to_insert = (data['url'], data['err_status'],
                              data['time'], data['text_found'])

            self.cursor.execute(sql_str, data_to_insert)

            self.logger.debug('Record inserted successfully into mobile table')

        except (Exception, psycopg2.Error) as error:
            breakpoint()
            self.logger.error(f'Failed to insert record into {self.table} table: {error}')

    def clean_up(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
