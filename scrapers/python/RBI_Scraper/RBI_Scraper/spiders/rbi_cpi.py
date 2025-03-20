# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import json
import os
import re
from datetime import datetime
from rich import print
import pandas as pd
import scrapy
from scrapy.exceptions import CloseSpider


class RbiCpiSpider(scrapy.Spider):
    name = "rbi_cpi"
    allowed_domains = ["website.rbi.org.in"]
    start_urls = ["https://website.rbi.org.in/web/rbi/publications/rbi-bulletin"]
    year_of_data = 0
    CSV_FILE = "rbi_cpi_last_date.csv"
    # Define file path
    jsonl_file_path = r"D:\Desktop\financial_data_pipeline\data\raw\RBI_data\CPI-Rural_Urban_Combined.json.jsonl"

    def parse(self, response):
        raw_date = response.xpath('(//span[@class="mtm_list_item_heading truncatedContent font-resized"])[2]/text()').get().strip()
        date_part = raw_date.split(" - ")[-1]
        extracted_date = datetime.strptime(date_part, "%b %d, %Y").date()

        # if os.path.exists(self.CSV_FILE):
        #     df = pd.read_csv(self.CSV_FILE)
        #     last_saved_date = datetime.strptime(df.iloc[0, 0], "%Y-%m-%d").date()  # Read and convert to date
        #
        #     if extracted_date == last_saved_date:
        #         self.log(extracted_date)
        #         raise CloseSpider("The new Consumer Survey is yet to be uploaded by the RBI.")

        # If new date, update CSV and proceed with scraping
        df = pd.DataFrame([[extracted_date]], columns=["date"])
        df.to_csv(self.CSV_FILE, index=False)

        latest_url = response.xpath('(//a[@class="mtm_list_item_heading"])[2]/@href').get()
        absolute_url = f'https://website.rbi.org.in{latest_url}'
        self.log(absolute_url)
        yield scrapy.Request(url=absolute_url, callback=self.parse_bulletin, cb_kwargs=dict(date=extracted_date))

    def parse_bulletin(self, response, date):

        relative_url = re.findall(r'href="([^"]+consumer-price-index[^"]*)"', response.text)[0]
        absolute_url = f'https://website.rbi.org.in{relative_url}'
        yield scrapy.Request(url=absolute_url, callback=self.parse_cpi, cb_kwargs=dict(date=date))

    def parse_cpi(self, response, date):
        year_extracted = date.year
        month_extracted = date.month

        # If the month is January (1) or February (2), subtract 1 from the year.
        self.year_of_data = year_extracted - 1 if month_extracted in [1, 2] else year_extracted

        print("year_of_data:", self.year_of_data)

        outer_table = response.xpath('//div[@class="tablebg"]/table')
        table = outer_table.xpath(".//table")

        categories_cpi = [
            "A) General Index",
            "A.1) Food and beverages",
            "A.1.1) Cereals and products",
            "A.1.10) Spices",
            "A.1.11) Non-alcoholic beverages",
            "A.1.12) Prepared meals, snacks, sweets etc.",
            "A.1.2) Meat and fish",
            "A.1.3) Egg",
            "A.1.4) Milk and products",
            "A.1.5) Oils and fats",
            "A.1.6) Fruits",
            "A.1.7) Vegetables",
            "A.1.8) Pulses and products",
            "A.1.9) Sugar and confectionery",
            "A.2) Pan, tobacco and intoxicants",
            "A.3) Clothing and footwear",
            "A.3.1) Clothing",
            "A.3.2) Footwear",
            "A.4) Housing",
            "A.5) Fuel and light",
            "A.6) Miscellaneous",
            "A.6.1) Household goods and services",
            "A.6.2) Health",
            "A.6.3) Transport and communication",
            "A.6.4) Recreation and amusement",
            "A.6.5) Education",
            "A.6.6) Personal Care and Effects",
            "B) Consumer Food Price Index"
        ]


        results = {}

        categories = categories_cpi

        # finding date for data
        rows = table.xpath('.//tbody/tr[td]')

        condition_met = False

    #     for row in rows:
    #
    #         # Get all td elements in the row
    #         tds = row.xpath('./td')
    #
    #         row_text = ''.join(tds.xpath('.//text()').getall()).strip()
    #
    #         # Apply condition only if it hasn't been met yet
    #         if not condition_met and (
    #                 "Survey period ended" in row_text or
    #                 re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\. \d{2}', row_text,
    #                           re.IGNORECASE)
    #         ):
    #             condition_met = True  # Mark condition as met
    #             date_selector = tds[-1]
    #             extracted_date = self.convert_money_supp_date(date_selector.xpath('string()').get().strip())
    #             break
    #
    #     # Extracting data
    #
    #     rows = table.xpath('.//tr[number(string(td[last()])) = number(string(td[last()]))]')
    #     for i, row in enumerate(rows):
    #
    #         data = row.xpath('./td//text()').getall()
    #         # Initialize table_name
    #         table_name = None
    #
    #         element = data[0]
    #
    #         # Check for partial matches using regex (case-insensitive)
    #         for category in categories:
    #             if re.search(rf'{re.escape(category)}', element, re.IGNORECASE):
    #                 table_name = category
    #                 print(f"Match found: {table_name}")
    #                 break
    #
    #         # handling edge cases
    #
    #         # if re.search(r'^Salary', element, re.IGNORECASE):
    #         #     table_name = "Salary/Wages (D-I)"
    #         #
    #         # if table_name is None:
    #         #     table_name = "Part-time/Contractual/Outsourced Employees"
    #
    #         if table_name is not None:
    #
    #             td_count = len(data)
    #
    #             # Check if row has more than 6 <td> elements
    #             if td_count > 5:
    #                 results[table_name] = float(data[-1])
    #
    #     self.check_duplicate_date(extracted_date)
    #
    #     json_data = {
    #         extracted_date: results
    #     }
    #
    #     json_line = json.dumps(json_data)
    #     print(json_line)
    #
    #     with open(self.jsonl_file_path, "a", encoding="utf-8") as file:
    #         file.write(json_line + "\n")
    #
    #     print(f"Data appended to {self.jsonl_file_path}")
    #
    # def convert_money_supp_date(self, month_day_of_data):
    #     try:
    #         # Remove any periods from the abbreviated month, e.g. "Dec. 27" -> "Dec 27"
    #         month_day_clean = month_day_of_data.replace('.', '')
    #
    #         # Combine into a date string like "2025 Dec 27"
    #         date_str = f"{self.year_of_data} {month_day_clean}"
    #
    #         # Parse the string into a datetime object. The format %Y for the year, %b for the abbreviated month, and %d for the day.
    #         constructed_date = datetime.strptime(date_str, "%Y %b %d")
    #         formatted_date = constructed_date.date().strftime("%Y-%m-%d")  # Format for JSON
    #         return formatted_date
    #
    #     except:
    #         pass
    #     # Return original if something doesn't match
    #     return month_day_of_data
    #
    # def check_duplicate_date(self, extracted_date):
    #     try:
    #         with open(self.jsonl_file_path, 'r', encoding='utf-8') as file:
    #             for line in file:
    #                 data_point = json.loads(line.strip())
    #                 if extracted_date in data_point:  # Check if the extracted_date is a key
    #                     raise CloseSpider(f'Data with date {extracted_date} is already present. Closing spider.')
    #     except FileNotFoundError:
    #         self.logger.warning(f'File not found: {self.jsonl_file_path}. Proceeding as new file.')
    #     except json.JSONDecodeError as e:
    #         self.logger.error(f'Error reading JSONL file: {e}')
