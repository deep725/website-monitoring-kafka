import aiohttp
import asyncio
import logging
import re
import time

from kafka_publisher import KafkaPublisher

class WebMonitorApp:
    def __init__(self, config, loop):
        # KafkaPublisher.__init__(self, config.kafka_bootstrap_servers, config.kafka_topic)
        self.__loop = loop
        self.__config = config
        self.__publisher = KafkaPublisher(config)
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_url_list(self):
        return self.__config.url_list

    def stop(self):
        self.__publisher.stop()

    async def run(self):
        await self.fetch_urls(self.get_url_list())

    async def fetch_urls(self, url_items):
        self.logger.debug("Starting monitoring...")
        while True:
            print('Starting iteration...')
            tasks = []
            for item in url_items:
                tasks.append(self.async_open(item))

            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(self.__config.monitoring_interval)

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
                                       timeout=aiohttp.ClientTimeout(total=10),
                                       raise_for_status=True) as resp:
                web_content = await resp.text()

        except aiohttp.client_exceptions.ClientConnectorError as err:
            err_status = "ConnectorError"
            self.logger.info(f"{err}")

        except aiohttp.client_exceptions.ClientResponseError as err:
            err_status = str(err.status)
            self.logger.info(f"Cannot find {item['url']}: {err_status}")

        except asyncio.TimeoutError:
            err_status = "TimedOut"
            self.logger.info(f'Timed out - {item["url"]} ')

        except Exception as error:
            err_status = "Unknown"
            self.logger.info(f'Exception thrown from {item["url"]}: {error}')

        finally:
            time_elapsed = round((self.__loop.time() - start) * 1000, 2)
            self.logger.info(f'...{item["url"][0:20]}:{time_elapsed}')

            self.__publisher.send({
                'url': item['url'],
                'err_status': err_status if err_status else '-',
                'time': time_elapsed,
                'text_found': self.find_txt_in_content(web_content, item['txt_to_find'])
            })
        
    def find_txt_in_content(self, web_content, text_to_find):
      txt_matches = re.findall(
          text_to_find, web_content) if web_content is not None else ''

      if len(txt_matches) == 0:
          return False
      else:
          return True