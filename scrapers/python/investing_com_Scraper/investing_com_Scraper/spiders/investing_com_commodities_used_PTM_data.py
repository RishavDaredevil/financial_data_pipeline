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


class InvestingComCommoditiesUsedPtmDataSpider(scrapy.Spider):
    name = "investing_com_commodities_used_PTM_data"
    allowed_domains = ["www.investing.com"]
    data_to_get_xlsx = "investing_com_commodities_used_PTM_data_to_Get.xlsx"
    CSV_FILE_Six_basket_currency = "investing_com_commodities_used_PTM_data_last_date.csv"
    json_file_path_for_investing_com_india_bonds_data = r"D:\Desktop\financial_data_pipeline\data\raw\investing_com\investing_com_commodities_used_PTM_data"
    custom_settings = {
        "USER_AGENT": None,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    def start_requests(self):

        # Generate dynamic start and end times
        today = datetime.today()
        start_time = (today - timedelta(days=500)).strftime("%Y-%m-%d")
        # Append " 23:59" for the end time as in your original URL
        end_time = today.strftime("%Y-%m-%d")

        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
            'cache-control': 'no-cache',
            'domain-id': 'www',
            'origin': 'https://www.investing.com',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.investing.com/',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        }

        params = {
            # 'start-date': start_time,
            'start-date': "2013-01-01",
            # 'end-date': end_time,
            'end-date': "2025-04-13",
            'time-frame': 'Daily',
            'add-missing-rows': 'false',
        }

        # Build the URL with query string using urlencode
        query_string = urlencode(params)

        # Read the data from the Excel file
        query_data = pd.read_excel(self.data_to_get_xlsx, sheet_name=0)

        # Iterate over each row in the DataFrame
        for index, row in query_data.iterrows():
            # Extract the 'names' value
            name = row['Commodity Name']
            id_numbers = row['instrument_id']

            # Now id_numbers is a list of strings like ['24004', ...]
            # You can now loop over these to make your Scrapy requests, e.g.:
            url = f"https://api.investing.com/api/financialdata/historical/{id_numbers}?{query_string}"
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

        filtered_records.extend(existing_data)

        # Write back to the JSON file
        with open(creating_path, "w") as f:
            json.dump(filtered_records, f, indent=4)

        print(f"Data appended successfully to {creating_path}")