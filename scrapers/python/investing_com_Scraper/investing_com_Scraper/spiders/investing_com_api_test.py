# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import json
import os
import re
import traceback
from datetime import datetime, timedelta
from rich import print
import pandas as pd
import scrapy
from scrapy.exceptions import CloseSpider
from urllib.parse import urlencode

class InvestingComApiTestSpider(scrapy.Spider):
    name = "investing_com_api_test"
    custom_settings = {
        "USER_AGENT": None,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    def start_requests(self):
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
            'cache-control': 'no-cache',
            'domain-id': 'in',
            'origin': 'https://in.investing.com',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://in.investing.com/',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        }

        params = {
            'start-date': '2017-01-10',
            'end-date': '2025-03-31',
            'time-frame': 'Daily',
            'add-missing-rows': 'false',
        }

        # Build the URL with query string using urlencode
        query_string = urlencode(params)
        url = f"https://api.investing.com/api/financialdata/historical/24014?{query_string}"

        for browser in ["chrome116"]:
            yield scrapy.Request(
                "https://httpbin.org/headers",
                headers=headers,
                callback=self.parse,
                dont_filter=True,
                meta={"impersonate": browser},
            )
    def parse(self, response):
        data = json.loads(response.body)
        print(data)