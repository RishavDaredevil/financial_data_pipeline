# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
# Need to schedule every 2 weeks but also check if this clashes with the monthly bulletin one

import json
import os
import re
from datetime import datetime
from rich import print
import pandas as pd
import scrapy
from scrapy.exceptions import CloseSpider
from RBI_Scraper.helper_func import safe_float


class RbiFortnightMoneySupplySpider(scrapy.Spider):
    name = "rbi_fortnight_money_supply"
    allowed_domains = ["rbi.org.in"]
    start_urls = ["https://rbi.org.in/Scripts/WSSViewDetail.aspx?TYPE=Section&PARAM1=7"]
    year_of_data = 0
    CSV_FILE = "rbi_fortnight_money_supply_last_date.csv"
    # Define file path
    jsonl_file_path = r"D:\Desktop\financial_data_pipeline\data\raw\RBI_data\money_supply.jsonl"

    def parse(self, response):
        # print(response.text)
        raw_date = response.xpath('(//th)[1]/text()').get().strip()
        extracted_date = datetime.strptime(raw_date, "%d %b %Y").date()
        print(extracted_date)

        if os.path.exists(self.CSV_FILE):
            df = pd.read_csv(self.CSV_FILE)
            last_saved_date = datetime.strptime(df.iloc[0, 0], "%Y-%m-%d").date()  # Read and convert to date

            if extracted_date == last_saved_date:
                self.log(extracted_date)
                raise CloseSpider("The new Consumer Survey is yet to be uploaded by the RBI.")

        # If new date, update CSV and proceed with scraping
        df = pd.DataFrame([[extracted_date]], columns=["date"])
        df.to_csv(self.CSV_FILE, index=False)

        latest_url = response.xpath('(//td)[1]/a/@href').get()
        absolute_url = f'https://rbi.org.in/Scripts/{latest_url}'
        print(absolute_url)
        self.log(absolute_url)
        yield scrapy.Request(url=absolute_url, callback=self.parse_fortnight_money_supply, cb_kwargs=dict(date=extracted_date))

    def parse_fortnight_money_supply(self, response, date):
        # print(response.text)
        year_extracted = date.year
        month_extracted = date.month
        day_extracted = date.day

        # If the month is January (1) or February (2), subtract 1 from the year.
        self.year_of_data = year_extracted - 1 if month_extracted in [1] and (day_extracted < 15)  else year_extracted

        print("year_of_data:", self.year_of_data)
        outer_table = response.xpath('//table[@class="tablebg"]')
        table = outer_table.xpath(".//table")

        categories_money_supply = [
            "Currency with the public",
            "Demand Deposits with Banks",
            "'Other' Deposits with Reserve Bank",
            "M1",
            "Post Office Savings Deposits",
            "M2",
            "Time Deposits with Banks",
            "M3",
            "Total Post Office Deposits",
            "M4"
        ]

        categories_not_in_fortnight_money_supply = ["Post Office Savings Deposits", "M2", "Total Post Office Deposits", "M4"]

        results = {}

        categories = categories_money_supply

        # finding date for data
        rows = table.xpath('.//tr[td]')

        condition_met = False

        for row in rows:

            # Get all td elements in the row
            tds = row.xpath('./td')

            row_text = ''.join(tds.xpath('.//text()').getall()).strip()
            # Apply condition only if it hasn't been met yet
            if not condition_met and (
                    "Survey period ended" in row_text or
                    re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\. \d{2}', row_text,
                              re.IGNORECASE)
            ):
                condition_met = True  # Mark condition as met
                date_selector = tds[1]
                extracted_date = self.convert_date_numeric_is_day(date_selector.xpath('string()').get().strip(),
                                                                  year_of_data = self.year_of_data)
                break

        # Extracting data

        rows = table.xpath('.//tr[number(string(td[last()])) = number(string(td[last()]))]')
        for i, row in enumerate(rows):

            data = row.xpath('./td//text()').getall()
            # Initialize table_name
            table_name = None

            element = data[0]

            # Check for partial matches using regex (case-insensitive)
            for category in categories:
                if re.search(rf'{re.escape(category)}', element, re.IGNORECASE):
                    table_name = category
                    print(f"Match found: {table_name}")
                    break

            # handling edge cases

            if re.search(r'Deposits with Reserve Bank', element, re.IGNORECASE):
                table_name = "'Other' Deposits with Reserve Bank"

            # if table_name is None:
            #     table_name = "Part-time/Contractual/Outsourced Employees"

            if table_name is not None:

                td_count = len(data)

                # Check if row has more than 6 <td> elements
                if td_count > 12:
                    results[table_name] = safe_float(data[2])

        results["M1"] = results["Currency with the public"] + results["Demand Deposits with Banks"] + results["'Other' Deposits with Reserve Bank"]

        for category in categories_not_in_fortnight_money_supply:
            if category in results:
                continue  # If the category is already a key, do nothing
            else:
                results[category] = None # Add the category with value "NA"

        # self.check_duplicate_date(extracted_date)

        json_data = {
            extracted_date : results
        }

        json_line = json.dumps(json_data)
        print(json_line)

        with open(self.jsonl_file_path, "a", encoding="utf-8") as file:
            file.write(json_line + "\n")

        print(f"Data appended to {self.jsonl_file_path}")

    def convert_date_numeric_is_day(self, month_day_of_data,year_of_data):
        try:
            # Remove any periods from the abbreviated month, e.g. "Dec. 27" -> "Dec 27"
            month_day_clean = month_day_of_data.replace('.', '')

            # Combine into a date string like "2025 Dec 27"
            date_str = f"{year_of_data} {month_day_clean}"

            # Parse the string into a datetime object. The format %Y for the year, %b for the abbreviated month, and %d for the day.
            constructed_date = datetime.strptime(date_str, "%Y %b %d")
            formatted_date = constructed_date.date().strftime("%Y-%m-%d")  # Format for JSON
            return formatted_date

        except:
            pass
        # Return original if something doesn't match
        return month_day_of_data

    def check_duplicate_date(self, extracted_date):
        try:
            with open(self.jsonl_file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    data_point = json.loads(line.strip())
                    if extracted_date in data_point:  # Check if the extracted_date is a key
                        raise CloseSpider(f'Data with date {extracted_date} is already present. Closing spider.')
        except FileNotFoundError:
            self.logger.warning(f'File not found: {self.jsonl_file_path}. Proceeding as new file.')
        except json.JSONDecodeError as e:
            self.logger.error(f'Error reading JSONL file: {e}')
