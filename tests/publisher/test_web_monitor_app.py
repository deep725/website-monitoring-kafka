import pytest
from main.publisher.web_monitor_app import WebMonitorApp
from main.publisher.kafka_publisher import KafkaPublisher
import asyncio, aiohttp
from aioresponses import aioresponses

@pytest.fixture()
def response_mock():
    with aioresponses() as mock:
        yield mock

@pytest.fixture
def web_monitor_app(cfg_read, mocker):
    loop = mocker.MagicMock()
    loop.time.return_value = 4
    
    producer = mocker.MagicMock()
    mocker.patch.object(KafkaPublisher, "get_producer", lambda x, y: producer)

    return WebMonitorApp(cfg_read, loop)

url_list = {
        "url": "http://www.google.com",
        "txt_to_find": "Additional products"
    }

class TestWebMonitorApp:
    @pytest.mark.asyncio 
    async def test_async_open_505(self, web_monitor_app, response_mock):
        response_mock.get(url_list['url'], status=500)
        result = await web_monitor_app.async_open(url_list)
        assert '500' == result['err_status']

    @pytest.mark.asyncio 
    async def test_async_open_Timeout(self, web_monitor_app, response_mock):
        response_mock.get(url_list['url'], exception=asyncio.TimeoutError)
        result = await web_monitor_app.async_open(url_list)
        assert 'Connection Error' == result['err_status']

    @pytest.mark.asyncio 
    async def test_async_open_connecting_error(self, web_monitor_app, response_mock):
        response_mock.get(url_list['url'], exception=aiohttp.ClientConnectorError(None, OSError()))
        result = await web_monitor_app.async_open(url_list)
        assert 'Connection Error' == result['err_status']
  
    @pytest.mark.asyncio 
    async def test_async_open_Exception(self, web_monitor_app, response_mock):
        response_mock.get(url_list['url'], exception=aiohttp.InvalidURL(''))
        result = await web_monitor_app.async_open(url_list)
        assert 'Unknown' == result['err_status']
  
    @pytest.mark.asyncio 
    async def test_async_open_text_found(self, web_monitor_app, response_mock):
        response_mock.get(url_list['url'], status=200, body='Text is Additional products')
        result = await web_monitor_app.async_open(url_list)
        assert '-' == result['err_status']
        assert True == result['text_found']

    @pytest.mark.asyncio 
    async def test_text_not_found(self, web_monitor_app, response_mock):
        response_mock.get(url_list['url'], status=200, body='Text is blank')
        result = await web_monitor_app.async_open(url_list)
        assert '-' == result['err_status']
        assert False == result['text_found']

    @pytest.mark.asyncio 
    async def test_fetch_urls(self, web_monitor_app, mocker, response_mock):
        mocker.patch.object(WebMonitorApp, 'get_url_list', lambda x: [url_list])
        response_mock.get(url_list['url'], status=200, body='Text is blank')
        result = await web_monitor_app.fetch_urls([url_list])
        web_monitor_app.stop()
        assert False == result[0]['text_found']

