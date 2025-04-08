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


class SpotPricesInIndiaDataSpider(scrapy.Spider):
    name = "spot_prices_in_india_data"
    allowed_domains = ["www.mcxindia.com"]
    start_urls = ["https://www.mcxindia.com/backpage.aspx/GetSpotMarketArchive"]
    data_to_get_xlsx = "Commodity_names.xlsx"
    CSV_FILE_commodities = "mcx_exchange_spot_price_commodities_data_last_date.csv"
    CSV_FILE_commodities_first_Date = "mcx_exchange_spot_price_commodities_data_first_date.csv"
    json_file_path_for_commodities = r"D:\Desktop\financial_data_pipeline\data\raw\mcx_exchange_data\commodities_spot_prices"

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': 'https://www.mcxindia.com',
        'Pragma': 'no-cache',
        'Referer': 'https://www.mcxindia.com/market-data/spot-market-price',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Cookie': 'device-referrer=https://www.google.com/; device-source=https://www.mcxindia.com/home; ASP.NET_SessionId=0bfr13kxrv3ukfgfsjy2qonl'
    }

    def start_requests(self):

        # Read the data from the Excel file
        query_data_commodities = pd.read_excel(self.data_to_get_xlsx)

        # Iterate over each row in the DataFrame
        for index, row in query_data_commodities.iterrows():

        # payload = json.dumps({
        #     "Product": "ALUMINI",
        #     "Location": "ALL",
        #     "Fromdate": "20170101",
        #     "Session": "0",
        #     "Todate": "20250401"
        # })

            # Extract the 'names' value
            name = row['Value']

            payload = json.dumps({
                "Product": name,
                "Location": "ALL",
                "Fromdate": "20000101",
                "Session": "0",
                "Todate": "20170102"
            })
            yield scrapy.Request(url=self.start_urls[0], method='POST',
                                 body=payload, headers=self.headers, cb_kwargs={"name": name})

    def parse(self, response, name):

        if response.status != 200:
            raise CloseSpider(f'{response.status} may be check and update the headers')
        json_response = json.loads(response.body)

        # Access the data array from the JSON structure
        data = json_response["d"]["Data"]

        # Create a list to store the formatted records
        records = []
        data_nf_names_list = []

        try:
            for item in data:
                # Extract the required fields
                symbol = item.get("Symbol")
                spot_price = item.get("TodaysSpotPrice")
                unit = item.get("Unit")
                location = item.get("Location")
                date_str = item.get("Date")

                # Extract the numeric part from the date string (e.g. "/Date(1743508075623)/")
                timestamp_match = re.search(r"\d+", date_str)
                if timestamp_match:
                    timestamp = int(timestamp_match.group())
                    # Check if the timestamp is in milliseconds (13 digits) or seconds (10 digits)
                    if len(timestamp_match.group()) == 13:
                        timestamp /= 1000  # Convert milliseconds to seconds

                    date_obj = datetime.fromtimestamp(timestamp)
                    formatted_date_iso = date_obj.strftime("%Y-%m-%d")
                else:
                    date_obj = None
                    formatted_date_iso = None

                # Append the extracted data into the records list
                records.append({
                    "date": formatted_date_iso,
                    "python_date": date_obj,
                    "Symbol": symbol,
                    "TodaysSpotPrice": spot_price,
                    "Unit": unit,
                    "Location": location
                })
        except :
            print(f'No data {name}')
            data_nf_names_list.append(name)

        try:
            # For demonstration, print the records
            print(records[0])
        except IndexError:
            print(f'IndexError {name}')
            data_nf_names_list.append(name)

        # Ensure we have at least one record
        if not records:
            self.logger.info(f"No records found for {name}")
            return

        how_to_filter = "Older"

        self.log(f"Filtering will happen with {how_to_filter} dates")

        if how_to_filter != "Older":
            # Find the latest date in the scraped data
            latest_date_obj = max(record["python_date"] for record in records)

            # Format the date as a string (choose a format consistent with your CSV; here we use ISO format)
            latest_date_str = latest_date_obj.strftime("%Y-%m-%d")

            # Read the CSV file that holds the last_date values
            try:
                df = pd.read_csv(self.CSV_FILE_commodities)
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
            df.to_csv(self.CSV_FILE_commodities, index=False)

            # Assuming records is a list of dictionaries with keys:
            # "date", "python_date", "Symbol", "TodaysSpotPrice", "Unit", "Location"
            # and stored_date_obj is a datetime object (or None).

            filtered_records = []
            for record in records:
                # If no stored_date exists, include all records.
                if stored_date_obj is None or record["python_date"] > stored_date_obj:
                    filtered_records.append({
                        "date": record["date"],
                        "Symbol": record["Symbol"],
                        "TodaysSpotPrice": record["TodaysSpotPrice"],
                        "Unit": record["Unit"],
                        "Location": record["Location"]
                    })
            # Now filtered_records contains only the entries with dates greater than stored_date_obj.

        else:
            # Calculate the oldest date in the newly scraped records
            scrape_oldest_obj = min(record["python_date"] for record in records)
            scrape_oldest_str = scrape_oldest_obj.strftime("%Y-%m-%d")

            # Read the CSV file that holds the oldest_date values
            try:
                df = pd.read_csv(self.CSV_FILE_commodities_first_Date)
            except FileNotFoundError:
                # If the file doesn't exist, create an empty DataFrame with proper columns
                df = pd.DataFrame(columns=["names", "oldest_date"])

            # Check if there's an entry for the current name and get the stored oldest date
            if name in df["names"].values:
                stored_date_str = df.loc[df["names"] == name, "oldest_date"].iloc[0]
                try:
                    stored_date_obj = datetime.strptime(stored_date_str, "%Y-%m-%d")
                except ValueError:
                    self.logger.error(f"Date format error in CSV for {name}: {stored_date_str}")
                    stored_date_obj = None
            else:
                stored_date_obj = None

            # Update the CSV file as needed based on the new scrape's oldest date
            if stored_date_obj:
                # If the new scrape includes an older date than the stored date, update the CSV
                if scrape_oldest_obj < stored_date_obj:
                    df.loc[df["names"] == name, "oldest_date"] = scrape_oldest_str
                    self.logger.info(f"Updated oldest_date for {name} to {scrape_oldest_str}")
                    stored_date_obj = scrape_oldest_obj  # Update our variable to the new value
                else:
                    self.logger.info(f"No new older data for {name}. Oldest date ({stored_date_str}) is up-to-date.")
                    raise CloseSpider(f"Up-to-date data for {name}")
            else:
                # No entry exists; add a new row with the current scrape's oldest date
                new_row = pd.DataFrame({"names": [name], "oldest_date": [scrape_oldest_str]})
                df = pd.concat([df, new_row], ignore_index=True)
                self.logger.info(f"Added new entry for {name} with oldest_date {scrape_oldest_str}")
                stored_date_obj = scrape_oldest_obj

            # Write the updated DataFrame back to the CSV file
            df.to_csv(self.CSV_FILE_commodities_first_Date, index=False)

            # Now, perform filtering on the records outside of the CSV logic
            # Keep only records with a date older than the stored oldest date.
            filtered_records = []
            for record in records:
                if record["python_date"] < stored_date_obj:
                    filtered_records.append({
                        "date": record["date"],
                        "Symbol": record["Symbol"],
                        "TodaysSpotPrice": record["TodaysSpotPrice"],
                        "Unit": record["Unit"],
                        "Location": record["Location"]
                    })

            # At this point, filtered_records contains only the entries with dates older than the stored oldest date.

            # # Build the result dictionary with the expected schema
            # # Format: { name: [ { "python_date": datetime_object, "value": close_value }, ... ] }
            # result = {name: records}

        creating_path = rf'{self.json_file_path_for_commodities}\{name}.json'

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

        print(data_nf_names_list)


