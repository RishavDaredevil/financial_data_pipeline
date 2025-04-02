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


class AllForexDataWithApiSpider(scrapy.Spider):
    name = "all_forex_data_with_api"
    allowed_domains = ["www.fxempire.com"]
    data_to_get_xlsx = "Currency Basket – Normalised Weights.xlsx"
    CSV_FILE_Six_basket_currency = "fxempire_forex_data_Six_basket_currency_last_date.csv"
    CSV_FILE_Forty_basket_currency = "fxempire_forex_data_Forty_basket_currency_last_date.csv"
    json_file_path_for_Forty_basket_currency = r"D:\Desktop\financial_data_pipeline\data\raw\fxempire_data\Forty_basket_currency"
    json_file_path_for_Six_basket_currency = r"D:\Desktop\financial_data_pipeline\data\raw\fxempire_data\Six_basket_currency"


    def start_requests(self):
        # Generate dynamic start and end times
        today = datetime.today()
        start_time = (today - timedelta(days=364)).strftime("%m/%d/%Y")
        # Append " 23:59" for the end time as in your original URL
        end_time = today.strftime("%m/%d/%Y")

        # Read the data from the Excel file
        query_data_Six_basket_currency = pd.read_excel(self.data_to_get_xlsx, sheet_name="Six_basket_currency")
        query_data_Forty_basket_currency = pd.read_excel(self.data_to_get_xlsx, sheet_name="Forty_basket_currency")

        # Iterate over each row in the DataFrame
        for index, row in query_data_Six_basket_currency.iterrows():
            # Extract the 'names' value
            name = row['Country']
            Symbols = row['Symbol']

            # Construct the URL
            url = f"https://www.fxempire.com/api/v1/en/currencies/chart/candles?Symbol={Symbols}&PriceType=Mid&period=24&Precision=Hours&StartTime={start_time}&EndTime={end_time}%2023:59&_fields=ChartBars.StartDate,ChartBars.EndDate,ChartBars.High,ChartBars.Low,ChartBars.StartTime,ChartBars.Open,ChartBars.Close,ChartBars.Volume"

            # Yield a request for each name
            yield scrapy.Request(
                url=url,
                callback=self.parse_Six_basket_currency,
                meta={'Currency': name,
                      'Symbols' : Symbols}  # You can pass additional metadata if needed
            )


        # Iterate over each row in the DataFrame
        for index, row in query_data_Forty_basket_currency.iterrows():
            # Extract the 'names' value
            name = row['Country']
            Symbols = row['Symbol']

            # Construct the URL
            url = f"https://www.fxempire.com/api/v1/en/currencies/chart/candles?Symbol={Symbols}&PriceType=Mid&period=24&Precision=Hours&StartTime={start_time}&EndTime={end_time}%2023:59&_fields=ChartBars.StartDate,ChartBars.EndDate,ChartBars.High,ChartBars.Low,ChartBars.StartTime,ChartBars.Open,ChartBars.Close,ChartBars.Volume"

            # Yield a request for each name
            yield scrapy.Request(
                url=url,
                callback=self.parse_Forty_basket_currency,
                meta={'Currency': name,
                      'Symbols' : Symbols}  # You can pass additional metadata if needed
            )


    def parse_Six_basket_currency(self, response):

        # Retrieve 'name' from response meta
        name = response.meta.get('Currency')

        # Load JSON data
        data = json.loads(response.body)

        # Extract dates and close values from the data, storing them in a list
        records = []
        for item in data:
            formatted_date_str = item["StartDate"]
            Open_value = item["Open"]
            High_value = item["High"]
            Low_value = item["Low"]
            Close_value = item["Close"]

            # Convert the formatted date string to a datetime object
            # %b: Month name (e.g. "Apr")
            # %d: Day of the month (e.g. "05")
            # %y: 2-digit year (e.g. "24" -> 2024)
            date_obj = datetime.strptime(formatted_date_str, "%m/%d/%Y")
            formatted_date_iso = date_obj.strftime("%Y-%m-%d")

            records.append({
                "date": formatted_date_iso,
                "python_date" : date_obj,
                "Open" : Open_value,
                "High" : High_value,
                "Low" : Low_value,
                "Close" : Close_value
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

        # # Build the result dictionary with the expected schema
        # # Format: { name: [ { "python_date": datetime_object, "value": close_value }, ... ] }
        # result = {name: records}

        creating_path = rf'{self.json_file_path_for_Six_basket_currency}\{name}.json'

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

    def parse_Forty_basket_currency(self, response):

        # Retrieve 'name' from response meta
        name = response.meta.get('Currency')

        # Load JSON data
        data = json.loads(response.body)

        # Extract dates and close values from the data, storing them in a list
        records = []
        for item in data:
            formatted_date_str = item["StartDate"]
            Open_value = item["Open"]
            High_value = item["High"]
            Low_value = item["Low"]
            Close_value = item["Close"]

            # Convert the formatted date string to a datetime object
            # %b: Month name (e.g. "Apr")
            # %d: Day of the month (e.g. "05")
            # %y: 2-digit year (e.g. "24" -> 2024)
            date_obj = datetime.strptime(formatted_date_str, "%m/%d/%Y")
            formatted_date_iso = date_obj.strftime("%Y-%m-%d")

            records.append({
                "date": formatted_date_iso,
                "python_date" : date_obj,
                "Open" : Open_value,
                "High" : High_value,
                "Low" : Low_value,
                "Close" : Close_value
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
            df = pd.read_csv(self.CSV_FILE_Forty_basket_currency)
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
        df.to_csv(self.CSV_FILE_Forty_basket_currency, index=False)

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

        # # Build the result dictionary with the expected schema
        # # Format: { name: [ { "python_date": datetime_object, "value": close_value }, ... ] }
        # result = {name: records}

        creating_path = rf'{self.json_file_path_for_Forty_basket_currency}\{name}.json'

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

