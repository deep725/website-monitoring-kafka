import psycopg2
CONSTANT = 23


def double():
    # breakpoint()
    return CONSTANT * 2


class DBClient:
    def __init__(self):

        # create a cursor
        self.cursor = self.connect_to_db()

    def connect_to_db(self):
        self.conn = psycopg2.connect(database="testdb", user="postgres",
                                     password="1q2w3e4r", host="192.168.21.42", port="5432")

        print("Opened database successfully")
        return self.conn.cursor()

    def create_tables(self):
        """ create tables in the PostgreSQL database"""
        commands = (
                """
			CREATE TABLE IF NOT EXISTS vendors (
				vendor_id SERIAL PRIMARY KEY,
				vendor_name VARCHAR(255) NOT NULL
			)
			""",
                """ CREATE TABLE IF NOT EXISTS parts (
					part_id SERIAL PRIMARY KEY,
					part_name VARCHAR(255) NOT NULL
					)
			""",
                """
			CREATE TABLE IF NOT EXISTS part_drawings (
					part_id INTEGER PRIMARY KEY,
					file_extension VARCHAR(5) NOT NULL,
					drawing_data BYTEA NOT NULL,
					FOREIGN KEY (part_id)
					REFERENCES parts (part_id)
					ON UPDATE CASCADE ON DELETE CASCADE
			)
			""",
                """
			CREATE TABLE IF NOT EXISTS vendor_parts (
					vendor_id INTEGER NOT NULL,
					part_id INTEGER NOT NULL,
					PRIMARY KEY (vendor_id , part_id),
					FOREIGN KEY (vendor_id)
						REFERENCES vendors (vendor_id)
						ON UPDATE CASCADE ON DELETE CASCADE,
					FOREIGN KEY (part_id)
						REFERENCES parts (part_id)
						ON UPDATE CASCADE ON DELETE CASCADE
			)
			""")

        for command in commands:
            self.cursor.execute(command)
