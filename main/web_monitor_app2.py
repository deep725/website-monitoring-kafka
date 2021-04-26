import aiohttp
import asyncio
import logging
import re
import time

# ToDo: SDSINGH : Remove this file

from kafka_publisher import KafkaPublisher

"""
This class uses HttpClientSession approach
"""


class WebMonitorApp2:
    def __init__(self, config, loop):
        # KafkaPublisher.__init__(self, config.kafka_bootstrap_servers, config.kafka_topic)
        self.__loop = loop
        self.__config = config
        self.__publisher = KafkaPublisher(
            config.kafka_bootstrap_servers, config.kafka_topic)
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run_pub(self):
        await self.fetch_urls(self.__config.url_list)

    def stop(self):
        self.__publisher.stop()

    async def fetch_urls(self, url_list):
        async with aiohttp.ClientSession(loop=self.__loop) as session:
            while True:
                tasks = []
                for url in url_list:
                    tasks.append(asyncio.ensure_future(
                        self.async_client_get(session, url)))

                await asyncio.gather(*tasks)

    async def async_client_get(self, session, url_data):
        start = time.time()
        try:
            async with session.get(url_data['url'], raise_for_status=True) as resp:
                web_content = await resp.text()

        except aiohttp.client_exceptions.ClientResponseError as err:
            err_status = err.status
            print(f"Cannot find {url_data['url']}: {err_status}")

        time_elapsed = int((time.time() - start) * 1000)

        self.__publisher.send({
            'url': url_data['url'],
            'err_status': err_status,
            'time': time_elapsed,
            'text_found': self.find_txt_in_content(web_content, url_data['txt_to_find'])
        })
        
    def find_txt_in_content(self, web_content, text_to_find):
        txt_matches = re.findall(
            text_to_find, web_content) if web_content is not None else ''

        if len(txt_matches) == 0:
            return False
        else:
            return True
