# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import json
import os
import re
from datetime import datetime
from rich import print
import pandas as pd
from scrapy.exceptions import CloseSpider
import scrapy


class RbiBankLendingSurveySpider(scrapy.Spider):
    name = "rbi_bank_lending_survey"
    allowed_domains = ["website.rbi.org.in"]
    start_urls = ["https://website.rbi.org.in/web/rbi/publications/articles?category=24927112"]
    CSV_FILE = "rbi_banking_survey.csv"
    # Define file path
    jsonl_file_path = r"D:\Desktop\financial_data_pipeline\data\raw\RBI_data\banking_survey.jsonl"

    def parse(self, response):
        # print(response.text)
        raw_date = response.xpath('(//div[@class="notification-date"]/span)[1]/text()').get().strip()
        extracted_date = datetime.strptime(raw_date, "%b %d, %Y").date()

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

        latest_url = response.xpath('(//a[@class="mtm_list_item_heading"])[1]/@href').get()
        self.log(latest_url)
        yield scrapy.Request(url=latest_url, callback=self.parse_services_and_infrastructure_survey,
                             cb_kwargs=dict(date=extracted_date))

    def parse_services_and_infrastructure_survey(self, response, date):
        global custom_mapping
        tables = response.xpath('//table[@class="tablebg"]')
        categories_banking_Loan_Demand = [
            "Loan Demand All Sectors",
            "Loan Demand Agriculture Sector",
            "Loan Demand Mining & Quarrying Sector",
            "Loan Demand Manufacturing Sector",
            "Loan Demand Infrastructure Sector",
            "Loan Demand Services Sector",
            "Loan Demand Retail/Personal Sector"]

        categories_banking_Loan_Terms_and_Conditions = [
            "Loan Terms and Conditions All Sectors",
            "Loan Terms and Conditions Agriculture Sector",
            "Loan Terms and Conditions Mining & Quarrying Sector",
            "Loan Terms and Conditions Manufacturing Sector",
            "Loan Terms and Conditions Infrastructure Sector",
            "Loan Terms and Conditions Services Sector",
            "Loan Terms and Conditions Retail/Personal Sector"
        ]
        results = {}

        for table_no, table in enumerate(tables):
            Category = "Banking"

            if table_no == 0:

                categories = categories_banking_Loan_Demand

                # finding date for data
                rows = table.xpath('.//tbody/tr[td]')

                condition_met = False

                for row in rows:

                    # Get all td elements in the row
                    tds = row.xpath('./td')

                    row_text = ''.join(row.xpath('.//text()').getall()).strip()
                    # Apply condition only if it hasn't been met yet
                    if not condition_met and (
                            re.search(r'Q\d.+', row_text,
                                      re.IGNORECASE)
                    ):
                        condition_met = True  # Mark condition as met
                        date_selector = tds[1]
                        extracted_date = self.convert_quarter_date(date_selector.xpath('string()').get().strip())
                        break

                if Category not in results:
                    results[Category] = {}

                # Extracting data

                rows = table.xpath('.//tr[number(string(td[last()])) = number(string(td[last()]))]')
                for i, row in enumerate(rows):

                    data = row.xpath('./td//text()').getall()
                    # Initialize table_name
                    table_name = None

                    element = data[0]

                    # Check for partial matches using regex (case-insensitive)
                    for category in categories:
                        if re.search(rf'\b{re.escape(element)}\b', category, re.IGNORECASE):
                            table_name = category
                            print(f"Match found: {table_name}")
                            break

                    # handling edge cases

                    # Custom mapping dictionary for one-to-one mapping
                    custom_mapping = {
                        "Mining and Quarrying": "Loan Demand Mining & Quarrying Sector"
                    }

                    # Check if the scraped_category matches a custom mapping
                    if element in custom_mapping:
                        table_name = custom_mapping[element]

                    if table_name not in results[Category]:
                        results[Category][table_name] = {}

                    td_count = len(data)

                    # Check if row has more than 6 <td> elements
                    if td_count > 4:
                        results[Category][table_name] = {
                            "Assessment - Net Res": float(data[2]),
                            "Expectation for 1 qtr ahead - Net Res": float(data[4])
                        }

            elif table_no == 1:

                categories = categories_banking_Loan_Terms_and_Conditions


                # finding date for data
                rows = table.xpath('.//tbody/tr[td]')

                condition_met = False

                for row in rows:

                    # Get all td elements in the row
                    tds = row.xpath('./td')

                    row_text = ''.join(row.xpath('.//text()').getall()).strip()
                    # Apply condition only if it hasn't been met yet
                    if not condition_met and (
                            re.search(r'Q\d.+', row_text,
                                      re.IGNORECASE)
                    ):
                        condition_met = True  # Mark condition as met
                        date_selector = tds[1]
                        extracted_date = self.convert_quarter_date(date_selector.xpath('string()').get().strip())
                        break

                if Category not in results:
                    results[Category] = {}

                # Extracting data

                rows = table.xpath('.//tr[number(string(td[last()])) = number(string(td[last()]))]')
                for i, row in enumerate(rows):

                    data = row.xpath('./td//text()').getall()
                    # Initialize table_name
                    table_name = None

                    element = data[0]

                    # Check for partial matches using regex (case-insensitive)
                    for category in categories:
                        if re.search(rf'\b{re.escape(element)}\b', category, re.IGNORECASE):
                            table_name = category
                            print(f"Match found: {table_name}")
                            break

                    # handling edge cases

                    # Custom mapping dictionary for one-to-one mapping
                    custom_mapping = {
                        "Mining and Quarrying": "Loan Terms and Conditions Mining & Quarrying Sector"
                    }

                    # Check if the scraped_category matches a custom mapping
                    if element in custom_mapping:
                        table_name = custom_mapping[element]

                    if table_name not in results[Category]:
                        results[Category][table_name] = {}

                    td_count = len(data)

                    # Check if row has more than 6 <td> elements
                    if td_count > 4:
                        results[Category][table_name] = {
                            "Assessment - Net Res": float(data[2]),
                            "Expectation for 1 qtr ahead - Net Res": float(data[4])
                        }



            elif table_no == 2:


                # Extracting data

                rows = table.xpath('.//tr[number(string(td[last()])) = number(string(td[last()]))]')
                for i, row in enumerate(rows):

                    categories = categories_banking_Loan_Demand
                    data = row.xpath('./td//text()').getall()
                    # Initialize table_name
                    table_name = None

                    element = data[0]

                    # Check for partial matches using regex (case-insensitive)
                    for category in categories:
                        if re.search(rf'\b{re.escape(element)}\b', category, re.IGNORECASE):
                            table_name = category
                            print(f"Match found: {table_name}")
                            break

                    # handling edge cases
                    custom_mapping = {
                        "Mining and Quarrying": "Loan Demand Mining & Quarrying Sector"
                    }

                    # Check if the scraped_category matches a custom mapping
                    if element in custom_mapping:
                        table_name = custom_mapping[element]

                    td_count = len(data)

                    # Check if row has more than 6 <td> elements
                    if td_count > 4:
                        results[Category][table_name].update({
                            "Expectation for 2 qtr ahead - Net Res": float(data[1]),
                            "Expectation for 3 qtr ahead - Net Res": float(data[2])
                        })

                    categories = categories_banking_Loan_Terms_and_Conditions

                    # Check for partial matches using regex (case-insensitive)
                    for category in categories:
                        if re.search(rf'\b{re.escape(element)}\b', category, re.IGNORECASE):
                            table_name = category
                            print(f"Match found: {table_name}")
                            break

                    # handling edge cases
                    custom_mapping = {
                        "Mining and Quarrying": "Loan Terms and Conditions Mining & Quarrying Sector"
                    }

                    # Check if the scraped_category matches a custom mapping
                    if element in custom_mapping:
                        table_name = custom_mapping[element]

                    # Check if row has more than 6 <td> elements
                    if td_count > 4:
                        results[Category][table_name].update({
                            "Expectation for 2 qtr ahead - Net Res": float(data[3]),
                            "Expectation for 3 qtr ahead - Net Res": float(data[4])
                        })


        # self.check_duplicate_date(extracted_date)

        json_data = {
            "date": extracted_date,
            "data": results
        }
        json_line = json.dumps(json_data)
        print(json_data)

        with open(self.jsonl_file_path, "a", encoding="utf-8") as file:
            file.write(json_line + "\n")

        print(f"Data appended to {self.jsonl_file_path}")

    def convert_quarter_date(self, q_str):
        try:
            if q_str.startswith("Q") and ":" in q_str:
                parts = q_str.split(":")
                quarter_num = parts[0][1]  # e.g. '4' from 'Q4'
                year_part = parts[1].split("-")[0]  # e.g. '2020' from '2020-21'
                if (quarter_num == '4'):
                    year_part = int(year_part) + 1
                mapping = {
                    "1": "06-01",
                    "2": "09-01",
                    "3": "12-01",
                    "4": "03-01"
                }
                md = mapping.get(quarter_num, "01-01")
                return f"{year_part}-{md}"
        except:
            pass
        # Return original if something doesn't match
        return q_str

    def check_duplicate_date(self, extracted_date):
        try:
            with open(self.jsonl_file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    data_point = json.loads(line.strip())
                    if data_point.get('date') == extracted_date:
                        raise CloseSpider(f'Data with date {extracted_date} is already present. Closing spider.')
        except FileNotFoundError:
            self.logger.warning(f'File not found: {self.jsonl_file_path}. Proceeding as new file.')
        except json.JSONDecodeError as e:
            self.logger.error(f'Error reading JSONL file: {e}')
