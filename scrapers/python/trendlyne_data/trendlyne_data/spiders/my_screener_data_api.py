# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import json
from datetime import datetime
from dateutil.utils import today

from rich import print
import pandas as pd
import scrapy
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector

class MyScreenerDataApiSpider(scrapy.Spider):
    name = "my_screener_data_api"
    allowed_domains = ["trendlyne.com"]
    pagenumber = 0
    start_urls = ["https://trendlyne.com/fundamentals/tl-all-in-one-screener-data-get/?format=json&groupName=all&groupType=all&perPageCount=200&screenpk=330921"]
    full_stock_data = []
    unstuctured_trendlyne_data = []
    jsonl_file_path_structured = r"D:\Desktop\financial_data_pipeline\data\raw\trendlyne_data\my_screener_data.jsonl"
    jsonl_file_path_unstuctured_trendlyne_data = r"D:\Desktop\financial_data_pipeline\data\raw\trendlyne_data\unstuctured_trendlyne_my_screener_data.jsonl"


    def start_requests(self):
        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://trendlyne.com/fundamentals/stock-screener/330921/main-data-mine-as-of-10-7-23/',
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            'Cookie': 'csrftoken=9ajAISs8deLGS8rROdIGO7LMLfjqJS9qNOg6yudn6PQxwuJJAcTvQEQ1d2ZK9b5y'}
        yield scrapy.Request(url=self.start_urls[0],headers=headers, callback=self.parse,meta={'headers': headers})

    def parse(self, response):

        headers = response.meta.get('headers')

        # Parse the JSON
        data = json.loads(response.text)

        no_of_headers = len(data['body']['tableHeaders'])

        print(no_of_headers)

        if data['head']['statusDescription'] != "Success":
            raise CloseSpider("request failed maybe check cookies")

        header_names = [
            "Stock", "Market Capitalization", "Annual 2y forward forecaster estimates EPS",
            "Annual 1y forward forecaster estimates EPS", "Basic EPS Annual", "Basic EPS Annual 1Yr Ago",
            "Forecaster Estimates industry reco", "Sector", "Industry", "Current Price",
            "Annual 2y forward forecaster estimates Revenue", "Annual 1y forward forecaster estimates Revenue",
            "Total Revenue Annual", "EnterpriseValue Annual", "EBIT Annual Per Share",
            "EV Per EBITDA Annual", "Outstanding Shares Adjusted", "Stock code", "ISIN", "BSE code", "NSE code"
        ]

        indexes = [2, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, None, 5, 7, 4, 3]

        self.unstuctured_trendlyne_data.append(data['body'])
        # Access the list of table headers
        is_there_next_page = data['head']['isNextPage']

        stock_data = data['body']['tableData']

        for stock in stock_data:
            # Extract the specified elements into a new list:
            stock_data_req = [(stock[i] if i is not None else None) for i in indexes]
            stock_data_with_headers = dict(zip(header_names, stock_data_req))
            self.full_stock_data.append(stock_data_with_headers)

        self.pagenumber += 1
        self.logger.info(f"Processing pagenumber: {self.pagenumber}")
        url = f'https://trendlyne.com/fundamentals/tl-all-in-one-screener-data-get/?format=json&groupType=all&pageNumber={self.pagenumber}&perPageCount=200&screenpk=330921'

        if is_there_next_page is True:
            yield scrapy.Request(url=url, headers=headers, callback=self.parse, meta={'headers': headers})
        else:
            json_data = {
                "date": today().strftime("%Y-%m-%d"),
                "data": self.full_stock_data
            }
            json_line = json.dumps(json_data)
            with open(self.jsonl_file_path_structured, "a", encoding="utf-8") as file:
                file.write(json_line + "\n")


            json_data = {
                "date": today().strftime("%Y-%m-%d"),
                "data": self.unstuctured_trendlyne_data
            }
            json_line = json.dumps(json_data)
            with open(self.jsonl_file_path_unstuctured_trendlyne_data, "a", encoding="utf-8") as file:
                file.write(json_line + "\n")





