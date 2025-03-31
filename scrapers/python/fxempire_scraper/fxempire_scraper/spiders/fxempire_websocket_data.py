##### Need to change header for Sec-WebSocket-Key

# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import logging
import websockets
from websocket import create_connection
import json
import os
import re
from datetime import datetime, timedelta
from rich import print
from scrapy.exceptions import CloseSpider
import asyncio


# Initialize the headers needed for the websocket connection
# headers = json.dumps({
#     'Date': 'Sun, 27 Aug 2017 09:42:15 GMT',
#     'Connection': 'upgrade',
#     'Host': 'www.kolumbus.no',
#     'Origin': 'https://www.kolumbus.no',
#     'Cookie': '.ASPXANONYMOUS=n3-SNfanLhJJsZsOZrYNFUMLRKd3aml7MYJ14xVgBBRZ0KLdZzWd5ncc3gEjCXtJ01XbHxnrt2lp0on3dJtFio6kl03eMDhJsRjZJ4MvgKr7atnbvuF6A-9UB9vJV-Uryhwj1RmxRhe9eIMrid3G5Q2; _gcl_au=1.1.1328331494.1562895478; _ga=GA1.2.17927177.1562895478; ASP.NET_SessionId=4iw2ui4kvnbkufrmxvk1isxd; _gid=GA1.2.1450924434.1562987695',
#     'Upgrade': 'websocket',
#     'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits',
#     'Sec-WebSocket-Key': 'a68fttH5Qvlftz/NQuCRZg==',
#     'Sec-WebSocket-Version': '13',
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
#     'Pragma': 'no-cache',
#     'Upgrade': 'websocket'
# })

##### Need to change header for Sec-WebSocket-Key
try:
    headers = {
        "Host": "one.fxempire.com",
        "Connection": "Upgrade",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Upgrade": "websocket",
        "Origin": "https://www.fxempire.com",
        "Sec-WebSocket-Version": "13",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
        "Sec-WebSocket-Key": "JJ79kd46Ixavc/oyhkgvmw==",
        "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits"
    }

    payload = json.dumps({
        "instruments": [
            "c-eur-usd",
            "c-usd-jpy",
            "c-usd-inr",
            "c-gbp-usd",
            "c-usd-chf",
            "c-usd-cad",
            "c-aud-usd",
            "c-gbp-jpy",
            "co-natural-gas",
            "co-gold",
            "co-wti-crude-oil",
            "co-brent-crude-oil",
            "co-copper",
            "cc-bitcoin",
            "cc-dogecoin",
            "i-spx",
            "i-vix",
            "s-aapl",
            "s-msft",
            "s-goog",
            "s-amzn",
            "s-meta",
            "s-nflx",
            "s-tsla"
        ],
        "type": "SUBSCRIBE"
    })

    async def parse():
        async with websockets.connect('wss://one.fxempire.com/') as ws:
            await ws.send(payload)
            while True:
                received = await ws.recv()
                print(json.loads(received))

    # Run the async function
    asyncio.run(parse())
except Exception as e:
    print(f'{e} may have happened cuz of header for Sec-WebSocket-Key. Need to change it ')


###### scrapy code below

# logging.getLogger("websockets").setLevel(logging.INFO)

# class FxempireWebsocketDataSpider(scrapy.Spider):
#     name = "fxempire_websocket_data"
#     allowed_domains = ["www.fxempire.com"]
#     start_urls = ["data:,"]  # avoid making an actual upstream request
#
#     async def parse(self, response):
#         async with websockets.connect("wss://echo.websocket.org") as ws:
#             await ws.send("Hello!")
#             received = await ws.recv()
#         yield {"received": received}