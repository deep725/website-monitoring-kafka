import json
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from socket import timeout

import logging
import re
import asyncio
import aiohttp
import signal
import sys
import time


def url_open(url):
    try:
        html_content = urlopen(
            url, timeout=3).read().decode()
    except timeout:
        print(f'socket timed out - {url} ')
    except HTTPError as err:
        print(err.code)
    except URLError as err:
        print(err.reason)
    else:
        matches = re.findall('regex of string to find', html_content)

        if len(matches) == 0:
            print('I did not find anything')
        else:
            print('My string is in the html')


# -------------------------------
async def on_signal(session, context, params):
    pass


async def async_client_get(session, url):
    start = time.time()
    async with session.get(url) as resp:
        end = int((time.time() - start) * 1000)
        if(resp.ok):
            print(f'......pass{resp.status}')

        else:
            print(f'......failed{resp.status}')
            return

        print(f'...{url[0:20]}:{end}')

        return await resp.text()


async def async_client_open(url_list):
    async with aiohttp.ClientSession(loop=loop) as session:
        i = 1
        while True:
            start = time.time()
            tasks = []
            for url in url_list:
                tasks.append(asyncio.ensure_future(
                    async_client_get(session, url)))

            await asyncio.gather(*tasks)
            end = int((time.time() - start) * 1000)
            print(f'{i}:{end}')
            i += 1

# -------------------------------

# https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/adminapi.py


def example_list(a):
    """ list topics, groups and cluster metadata """

    what = "all"

    md = a.list_topics(timeout=10)

    print("Cluster {} metadata (response from broker {}):".format(
        md.cluster_id, md.orig_broker_name))

    if what in ("all", "brokers"):
        print(" {} brokers:".format(len(md.brokers)))
        for b in iter(md.brokers.values()):
            if b.id == md.controller_id:
                print("  {}  (controller)".format(b))
            else:
                print("  {}".format(b))

    if what in ("all", "topics"):
        print(" {} topics:".format(len(md.topics)))
        for t in iter(md.topics.values()):
            if t.error is not None:
                errstr = ": {}".format(t.error)
            else:
                errstr = ""

            print("  \"{}\" with {} partition(s){}".format(
                t, len(t.partitions), errstr))

            for p in iter(t.partitions.values()):
                if p.error is not None:
                    errstr = ": {}".format(p.error)
                else:
                    errstr = ""

                print("partition {} leader: {}, replicas: {},"
                      " isrs: {} errstr: {}".format(p.id, p.leader, p.replicas,
                                                    p.isrs, errstr))


def create_partition(num_partitions):
    # Create Admin client
    a = AdminClient({'bootstrap.servers': '192.168.1.42:9092'})
    example_list(a)

    new_parts = [NewPartitions('demo-topic', int(num_partitions))]

    # Try switching validate_only to True to only validate the operation
    # on the broker but not actually perform it.
    fs = a.create_partitions(new_parts, validate_only=False)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Additional partitions created for topic {}".format(topic))
        except Exception as e:
            print("Failed to add partitions to topic {}: {}".format(topic, e))


# create_partition(5)
p = Producer({
    'bootstrap.servers': '192.168.1.42:9092'
})


def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write(
            '%% Message delivered to %s [%d] @ %d\n' % (msg.topic(), msg.partition(), msg.offset()))


def publishit(func):
    async def inner(url):
        x = await func(url)
        print(f"I got decorated-{url}:{x}")
        p.produce('demo-topic', key=url, value=json.dumps(x).encode('utf-8'),
                  callback=delivery_callback)
    return inner


def timeit(func):
    async def inner(url):
        print(f"I got time-{url}")
        start = loop.time()
        x = await func(url)
        end = round((loop.time() - start) * 1000, 2)
        print(f"I got time-{url}:{end}")

    return inner


@ timeit
async def async_request(url):
    start = time.time()
    async with aiohttp.request('GET',
                               url, timeout=aiohttp.ClientTimeout(total=34), raise_for_status=True) as resp:
        end = int((time.time() - start) * 1000)
        print(f'...{url[0:20]}:{end}')
        return await resp.text()

"""
Criteria needs to be cleared whether app should also store any other status apart from HTTP Status codes. For example if url isn't valid, then what should be the action
"""


@ publishit
async def async_open(url):
    web_content = None
    err_status = None
    start = loop.time()
    try:
        async with aiohttp.request('GET',
                                   url, timeout=aiohttp.ClientTimeout(total=62), raise_for_status=True) as resp:
            web_content = await resp.text()

    except aiohttp.client_exceptions.ClientConnectorError:
        print(f"Cannot connect to {url}")

    except aiohttp.client_exceptions.ClientResponseError as err:
        err_status = err.status
        print(f"Cannot find {url}: {err_status}")

    except asyncio.TimeoutError:
        print(f'Timed out - {url} ')

    except:
      print(f'xxx ')

    finally:
        time_elapsed = round((loop.time() - start) * 1000, 2)
        print(f'...{url[0:20]}:{time_elapsed}')
        txt_matches = re.findall(
            'Detect Indentation', web_content) if web_content is not None else ''

        if len(txt_matches) == 0:
            print('Text Not Found')
        else:
            print('Text Found')

        return {
            'url': url,
            'err_status': err_status,
            'time': time_elapsed,
            'text_found': False if len(txt_matches) == 0 else True
        }


async def fetch_urls(url_items):
    logging.debug("A Debug Logging Message")
    i = 1
    while True:
        start = time.time()
        start2 = loop.time()
        tasks = []
        for item in url_items:
            tasks.append(async_open(item['url']))

        await asyncio.gather(*tasks, return_exceptions=True)
        end = int((time.time() - start) * 1000)
        print(f'{i}:{end}-{round((loop.time() - start2) * 1000, 2)}')
        i += 1
        # break
        # await asyncio.sleep(2)

# url_open("http://192.168.1.48")
# url_open("http://google.com/dfd")
# url_open("https://google.com/")
# print("----")
# loop = asyncio.get_event_loop()
# loop.run_until_complete(async_open("http://192.168.1.48"))
# loop.run_until_complete(async_open("http://google.com/dfd"))
# loop.run_until_complete(async_open(
#     "https://stackoverflow.com/questions/49167053/how-do-i-change-vscode-to-indent-4-spaces-instead-of-default-2"))
# loop.close()


def signal_handler(signal, frame):
    loop.stop()

    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
loop = asyncio.get_event_loop()
url_list = [{"url": "http://pennow.tech"},
            {"url": "https://stackoverflow.com/questions/49167053/how-do-i-change-vscode-to-indent-4-spaces-instead-of-default-2"},
            {"url": "http://google.com/cc"},
            {"url": "ftp://yoox.com"},
            {"url": "http://underarmor.com"}]
loop.run_until_complete(fetch_urls(url_list))

# Wait until all messages have been delivered

# loop.run_until_complete(async_client_open(url_list))
# asyncio.ensure_future(async_open("http://192.168.1.48", loop))

# all_groups = asyncio.gather(*tasks, return_exceptions=True)
# results = loop.run_until_complete(all_groups)
# print(results)
