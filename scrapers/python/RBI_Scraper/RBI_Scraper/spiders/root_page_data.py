# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want

import os
import json
import logging
import scrapy
from rich import print
from RBI_Scraper.items import RBI_root_page, RBIItemLoader



class RootPageDataSpider(scrapy.Spider):
    name = "root_page_data"
    allowed_domains = ["rbi.org.in"]
    start_urls = ["https://rbi.org.in/#mainsection"]
    output_dir = r"D:\Desktop\financial_data_pipeline\data\raw\RBI_data"
    output_file = os.path.join(output_dir, "root_page_data.json")

    def parse(self, response):
        tables = response.xpath('//div[@id="wrapper"]//div[@class="accordionContent"]')
        section_values = [
            "Policy_Rates",
            "Reserve_Ratios",
            "Exchange_Rates",
            "Lending_Deposit_Rates",
            "Market_Trends"
        ]
        item = RBI_root_page()
        for table,section in zip(tables,section_values):
            table_rows = table.xpath('.//tr')
            loader = RBIItemLoader(item,selector=table_rows)
            item[section] = loader.load_tr_to_dict(table_rows)

        self.save_to_json(item)

    def save_to_json(self, item):
        # Create directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

        # Save the item to the JSON file
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(dict(item), f, ensure_ascii=False, indent=4)

        self.log(f"Data saved to {self.output_file}")

