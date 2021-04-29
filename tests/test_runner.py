

from unittest import mock
from src.runner import Runner

class TestMain:
    @mock.patch('src.runner.WebMonitorApp', autospec=True)
    @mock.patch('src.runner.asyncio', autospec=True) 
    def test_web_monitor_proc(self, asyncio_mock, web_monitor_app_mock, cfg_read):
        runner = Runner()
        runner.web_monitor_proc(cfg_read)

        asyncio_mock.get_event_loop.assert_called_once()
        loop_mock = asyncio_mock.get_event_loop.return_value
        loop_mock.stop.assert_called_once()
        loop_mock.run_until_complete.assert_called_once()
        
        web_monitor_app_mock.return_value.run.assert_called_once()
        web_monitor_app_mock.return_value.stop.assert_called_once()

    @mock.patch('src.runner.PgSQLSink', autospec=True)
    @mock.patch('src.runner.StatsConsumerApp', autospec=True)
    @mock.patch('src.runner.asyncio', autospec=True) 
    def test_stats_consumer_proc(self, asyncio_mock, consumer_app_mock, db_mock, cfg_read):
        runner = Runner()
        runner.stats_consumer_proc(cfg_read)

        asyncio_mock.get_event_loop.assert_called_once()
        loop_mock = asyncio_mock.get_event_loop.return_value
        loop_mock.stop.assert_called_once()
        loop_mock.run_until_complete.assert_called_once()
        
        db_mock.return_value.clean_up.assert_called_once()

        consumer_app_mock.return_value.run.assert_called_once()
        consumer_app_mock.return_value.stop.assert_called_once()


    def test_run_proc(self, cfg_read):
        with mock.patch('src.runner.multiprocessing', autospec=True) as multi_process_mock:
            runner = Runner()
            procs = [runner.web_monitor_proc]
            runner.run_procs(procs, cfg_read)

            multi_process_mock.Process.assert_called_once_with(target=procs[0], args=(cfg_read,))
            multi_process_mock.Process.return_value.join.assert_called_once()
