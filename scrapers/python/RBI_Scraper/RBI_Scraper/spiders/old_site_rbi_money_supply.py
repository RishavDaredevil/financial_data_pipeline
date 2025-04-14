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
from RBI_Scraper.helper_func import safe_float


class OldSiteRbiMoneySupplySpider(scrapy.Spider):
    name = "old_site_rbi_money_supply"
    allowed_domains = ["rbi.org.in"]
    start_urls = ["https://rbi.org.in/Scripts/BS_ViewBulletin.aspx"]
    year_of_data = 0
    CSV_FILE = "rbi_money_supply_last_date.csv"
    # Define file path
    jsonl_file_path = r"D:\Desktop\financial_data_pipeline\data\raw\RBI_data\money_supply.jsonl"

    def parse(self, response):
        date_text = response.xpath('//table[@class="tablebg"]//td[@class="tableheader"]/b/following-sibling::text()[1]').get().strip()
        if date_text:
            # Clean up the text by stripping extra whitespace
            date_text = date_text.strip()
            self.logger.info("Extracted Date: %s", date_text)
            extracted_date = datetime.strptime(date_text, "%b %d, %Y").date()

        if os.path.exists(self.CSV_FILE):
            df = pd.read_csv(self.CSV_FILE)
            last_saved_date = datetime.strptime(df.iloc[0, 0], "%Y-%m-%d").date()  # Read and convert to date

            if extracted_date == last_saved_date:
                self.log(extracted_date)
                raise CloseSpider("The new Consumer Survey is yet to be uploaded by the RBI.")

        # If new date, update CSV and proceed with scraping
        df = pd.DataFrame([[extracted_date]], columns=["date"])
        df.to_csv(self.CSV_FILE, index=False)


        relative_url = response.xpath('//a[contains(text(), "Money Stock Measures")]/@href').get()
        absolute_url = f'https://rbi.org.in/Scripts/{relative_url}'
        self.log(absolute_url)
        yield scrapy.Request(url=absolute_url, callback=self.parse_money_supply, cb_kwargs=dict(date=extracted_date))

    def parse_money_supply(self, response, date):
        year_extracted = date.year
        month_extracted = date.month

        # If the month is January (1) or February (2), subtract 1 from the year.
        self.year_of_data = year_extracted - 1 if month_extracted in [1, 2] else year_extracted

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
                date_selector = tds[-1]
                extracted_date = self.convert_money_supp_date(date_selector.xpath('string()').get().strip())
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

            if re.search(r'Post Oﬃce Saving Bank Deposits', element, re.IGNORECASE):
                table_name = "Post Office Savings Deposits"
                print(f"Match found: {table_name}")

            if re.search(r'Total Post Oﬃce Deposits', element, re.IGNORECASE):
                table_name = "Total Post Office Deposits"
                print(f"Match found: {table_name}")

            if table_name is not None:

                td_count = len(data)

                # Check if row has more than 6 <td> elements
                if td_count > 5:
                    results[table_name] = float(data[-1])

        json_data = {
            extracted_date : results
        }

        self.check_duplicate_date(extracted_date,json_data)

        json_line = json.dumps(json_data)

        # with open(self.jsonl_file_path, "a", encoding="utf-8") as file:
        #     file.write(json_line + "\n")
        #
        # print(f"Data appended to {self.jsonl_file_path}")

    def convert_money_supp_date(self, month_day_of_data):
        try:
            # Remove any periods from the abbreviated month, e.g. "Dec. 27" -> "Dec 27"
            month_day_clean = month_day_of_data.replace('.', '')

            # Combine into a date string like "2025 Dec 27"
            date_str = f"{self.year_of_data} {month_day_clean}"

            # Parse the string into a datetime object. The format %Y for the year, %b for the abbreviated month, and %d for the day.
            constructed_date = datetime.strptime(date_str, "%Y %b %d")
            formatted_date = constructed_date.date().strftime("%Y-%m-%d")  # Format for JSON
            return formatted_date

        except:
            pass
        # Return original if something doesn't match
        return month_day_of_data

    def check_duplicate_date(self, extracted_date, new_data):
        """
        Check the JSONL file to see if a record for extracted_date exists.
        If it exists and some values are "NA", update those keys with the corresponding data from new_data.

        Args:
            extracted_date (str): The date key to look for in the JSONL file.
            new_data (dict): Dictionary with keys to update and their new values.
                             Only keys with "NA" in the current record will be updated.
        """
        # Read all lines from the JSONL file
        record_updated = False
        records_dict = {}
        try:
            with open(self.jsonl_file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    line = line.strip()
                    # Each line is a JSON object with a single key (the date)
                    record = json.loads(line)
                    data_point = json.loads(line)
                    for date_key, data in record.items():
                        # Store/overwrite existing records with key date_key.
                        records_dict[date_key] = data
                    if extracted_date in data_point:  # Check if the extracted_date is a key
                        # Iterate over keys in the record for that date
                        for key, value in data_point[extracted_date].items():
                            # If the value is "NA" and new_data provides an update, change it
                            if (value == "NA" or value is None) and key in new_data[extracted_date]:
                                record_updated = True
                                # Remove any old record(s) for the specified date and update with the new record.
                                records_dict[extracted_date] = new_data[extracted_date]
                        # Convert back to JSON string after potential updates
                        updated_line = json.dumps(records_dict[extracted_date])
                        print(updated_line)

            # If we updated the record, write back all records to the file
                if record_updated is True:
                    # Sort the records by date (the date format allows for correct lexicographical sorting)
                    sorted_date_keys = sorted(records_dict.keys())
                    sorted_records = []

                    for date_key in sorted_date_keys:
                        sorted_records.append({date_key: records_dict[date_key]})

                    # Write the sorted records back to the JSONL file.
                    with open(self.jsonl_file_path, 'w', encoding='utf-8') as file:
                        for record in sorted_records:
                            file.write(json.dumps(record) + "\n")
                    raise CloseSpider(f"Record for {extracted_date} updated with new values for keys with 'NA'.")
                else:
                    self.logger.info(
                        f"No 'NA' values found for date {extracted_date} to update. If a record exists, it is already complete.")
                    raise CloseSpider(f'Data with date {extracted_date} is already present with full data according to info above. Closing spider.')


        except FileNotFoundError:
            self.logger.warning(f'File not found: {self.jsonl_file_path}. Proceeding as new file.')
        except json.JSONDecodeError as e:
            self.logger.error(f'Error reading JSONL file: {e}')
