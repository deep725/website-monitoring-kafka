import aiohttp
import asyncio
import logging
import re

from .kafka_publisher import KafkaPublisher

class WebMonitorApp:
    def __init__(self, config, loop):
        self.__loop = loop
        self.__config = config
        self.__publisher = KafkaPublisher(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__run = False

    def get_url_list(self):
        return self.__config.url_list

    def stop(self):
        self.logger.info('Stoping iteration...')
        self.__publisher.stop()

    async def run(self):
        while True:
          await self.fetch_urls(self.get_url_list())
          await asyncio.sleep(self.__config.monitoring_interval)

    async def fetch_urls(self, url_items):
        tasks = []
        self.logger.info('Starting next monitoring iteration...')
        for item in url_items:
            tasks.append(self.async_open(item))

        return await asyncio.gather(*tasks, return_exceptions=True)

    """
    Criteria needs to be cleared whether app should also store any other
    status apart from HTTP Status codes. For example if url isn't valid,
    then what should be the action
    """
    async def async_open(self, item):
        web_content = None
        err_status = '-'
        start = self.__loop.time()
        try:
            async with aiohttp.request('GET',
                                       item['url'],
                                       timeout=aiohttp.ClientTimeout(total=10)
                                       ) as resp:
                if resp.status >= 400:
                    err_status = str(resp.status)
                    self.logger.info(f"Cannot find {item['url']}: {err_status}")
                
                web_content = await resp.text()

        except (aiohttp.ClientConnectorError, asyncio.TimeoutError) as error:
            err_status = "Connection Error"
            self.logger.error(f'{err_status}: {item["url"]} {error}')

        except Exception as error:
            err_status = "Unknown"
            self.logger.error(f'Exception thrown from {item["url"]}: {error}')

        finally:
            time_elapsed = round((self.__loop.time() - start) * 1000, 2)
            self.logger.info(f'...{item["url"][0:20]}:{time_elapsed}')

            msg = {
                'url': item['url'],
                'err_status': err_status if err_status else '-',
                'time': time_elapsed,
                'text_found': self.find_txt_in_content(web_content, item['txt_to_find'])
            }

            self.__publisher.send(msg)
            return msg
        
    def find_txt_in_content(self, web_content, text_to_find):
        txt_matches = re.findall(
            text_to_find, web_content) if web_content is not None else ''

        if len(txt_matches) == 0:
            return False
        else:
            return True
