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


class InvestingComIndiaBondsDataSpider(scrapy.Spider):
    name = "investing_com_india_bonds_data"
    allowed_domains = ["in.investing.com"]
    start_urls = ["https://in.investing.com/rates-bonds/india-government-bonds"]
    CSV_FILE_Six_basket_currency = "investing_com_india_bonds_data_last_date.csv"
    json_file_path_for_investing_com_india_bonds_data = r"D:\Desktop\financial_data_pipeline\data\raw\investing_com\investing_com_india_bonds_data"
    custom_settings = {
        "USER_AGENT": None,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    def parse(self, response):
        # Generate dynamic start and end times
        today = datetime.today()
        start_time = (today - timedelta(days=364)).strftime("%Y-%m-%d")
        # Append " 23:59" for the end time as in your original URL
        end_time = today.strftime("%Y-%m-%d")

        # Extract all the 'pair' ids
        pair_ids = response.xpath("//tr[starts-with(@id, 'pair')]/@id").getall()
        names = response.xpath("//tr[starts-with(@id, 'pair')]//a[starts-with(@title, 'Ind')]/@title").getall()
        # Remove the "pair_" prefix to get only the numeric part
        id_numbers = [pid.replace("pair_", "") for pid in pair_ids]

        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
            'cache-control': 'no-cache',
            'domain-id': 'in',
            'origin': 'https://in.investing.com',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://in.investing.com/',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        }

        params = {
            'start-date': start_time,
            'end-date': end_time,
            'time-frame': 'Daily',
            'add-missing-rows': 'false',
        }

        # Build the URL with query string using urlencode
        query_string = urlencode(params)

        # Now id_numbers is a list of strings like ['24004', ...]
        # You can now loop over these to make your Scrapy requests, e.g.:
        for name,id_num in zip(names,id_numbers):
            url = f"https://api.investing.com/api/financialdata/historical/{id_num}?{query_string}"
            yield scrapy.Request(
                url,
                headers=headers,
                callback=self.parse_each_pair_id,
                dont_filter=True,
                meta={"impersonate": "chrome116"},
                cb_kwargs={"name": name}
            )

    def parse_each_pair_id(self, response, name):
        json_data = json.loads(response.body)

        records = []

        for item in json_data["data"]:
            # Extract the required fields
            formatted_date_str = item["rowDate"]
            Open_value = item["last_open"]
            High_value = item["last_max"]
            Low_value = item["last_min"]
            Close_value = item["last_close"]

            # Convert the formatted date string ("Mar 15, 2021") to a datetime object.
            # The format specifier "%b %d, %Y" corresponds to "Mar 15, 2021"
            date_obj = datetime.strptime(formatted_date_str, "%b %d, %Y")
            formatted_date_iso = date_obj.strftime("%Y-%m-%d")

            records.append({
                "date": formatted_date_iso,
                "python_date": date_obj,
                "Open": Open_value,
                "High": High_value,
                "Low": Low_value,
                "Close": Close_value
            })

        # Ensure we have at least one record
        if not records:
            self.logger.info(f"No records found for {name}")
            return

        # Find the latest date in the scraped data
        latest_date_obj = max(record["python_date"] for record in records)

        # Format the date as a string (choose a format consistent with your CSV; here we use ISO format)
        latest_date_str = latest_date_obj.strftime("%Y-%m-%d")

        # Read the CSV file that holds the last_date values
        try:
            df = pd.read_csv(self.CSV_FILE_Six_basket_currency)
        except FileNotFoundError:
            # If the file doesn't exist, create an empty DataFrame with proper columns
            df = pd.DataFrame(columns=["names", "last_date"])

        # Check if there's an entry for the current name
        if name in df["names"].values:
            # Extract the stored last_date for this name
            stored_date_str = df.loc[df["names"] == name, "last_date"].iloc[0]
            try:
                stored_date_obj = datetime.strptime(stored_date_str, "%Y-%m-%d")
            except ValueError:
                self.logger.error(f"Date format error in CSV for {name}: {stored_date_str}")
                stored_date_obj = None

            # If the stored date equals the latest scraped date, close the spider
            if stored_date_obj and stored_date_obj == latest_date_obj:
                self.logger.info(f"No new data for {name}. Latest date ({latest_date_str}) is up-to-date.")
                raise CloseSpider(f"Up-to-date data for {name}")

            else:
                # Update the CSV with the new latest date for this name
                df.loc[df["names"] == name, "last_date"] = latest_date_str
                self.logger.info(f"Updated last_date for {name} to {latest_date_str}")

        else:
            # Append a new row if the name is not present in the CSV
            stored_date_obj = None
            new_row = pd.DataFrame({"names": [name], "last_date": [latest_date_str]})
            df = pd.concat([df, new_row], ignore_index=True)
            self.logger.info(f"Added new entry for {name} with last_date {latest_date_str}")

        # Write the updated DataFrame back to the CSV file
        df.to_csv(self.CSV_FILE_Six_basket_currency, index=False)

        # Create a new dictionary of records containing only entries with dates greater than the stored date.
        filtered_records = []
        for record in records:
            # If no stored_date exists, then include all records.
            if stored_date_obj is None or record["python_date"] > stored_date_obj:
                # Use a consistent string format for the dictionary key.
                filtered_records.append({
                    "date": record["date"],
                    "Open": record["Open"],
                    "High": record["High"],
                    "Low": record["Low"],
                    "Close": record["Close"]
                })

        creating_path = rf'{self.json_file_path_for_investing_com_india_bonds_data}\{name}.json'

        # Load the existing JSON data if it exists; otherwise, initialize an empty list.
        if os.path.exists(creating_path):
            with open(creating_path, "r") as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []
        else:
            existing_data = []

        existing_data.extend(filtered_records)

        # Write back to the JSON file
        with open(creating_path, "w") as f:
            json.dump(existing_data, f, indent=4)

        print(f"Data appended successfully to {creating_path}")