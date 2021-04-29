import pytest
from unittest import mock

from src.consumer.pgsql_sink import PgSQLSink


@pytest.fixture
@mock.patch('src.consumer.pgsql_sink.psycopg2', autospec=True)
def create_db_mock(psql_mock, cfg_read):
    return psql_mock, PgSQLSink(cfg_read)


class TestPgSQLSink:
    def test_db_create(self, create_db_mock):
        psql_mock, db_obj = create_db_mock

        assert psql_mock.connect.return_value == db_obj.connection()

        psql_mock.connect.assert_called_once()
        psql_mock.connect.return_value.cursor.assert_called_once()

    def test_db_create_table(self, create_db_mock):
        psql_mock, db_obj = create_db_mock

        db_obj.create_table()

        psql_mock.connect.return_value.cursor.return_value.execute.assert_called()

    def test_db_clean(self, create_db_mock):
        psql_mock, db_obj = create_db_mock

        db_obj.clean_up()

        psql_mock.connect.return_value.cursor.return_value.close.assert_called()

    @pytest.mark.asyncio
    async def test_db_insert(self, create_db_mock, cfg_read):
        psql_mock, db_obj = create_db_mock
        data = {"url": "http://google.com/cc", "err_status": "404",
                "time": "47.0", "text_found": "yes"}

        await db_obj.insert(data)

        psql_mock.connect.return_value.cursor.return_value.execute.assert_called()
