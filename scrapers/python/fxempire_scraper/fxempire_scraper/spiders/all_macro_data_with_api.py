# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import json
import os
import re
import traceback
from datetime import datetime
from rich import print
import pandas as pd
import scrapy
from scrapy.exceptions import CloseSpider


class AllMacroDataWithApiSpider(scrapy.Spider):
    name = "all_macro_data_with_api"
    allowed_domains = ["www.fxempire.com"]
    data_to_get_xlsx = "fxempire_data_to_Get.xlsx"
    CSV_FILE = "rbi_fxempire_last_date.csv"
    json_file_path_for_data_has_stopped_updating = r"D:\Desktop\financial_data_pipeline\data\raw\fxempire_data\data_has_stopped_updating"
    json_file_path_for_Has_latest_data_available = r"D:\Desktop\financial_data_pipeline\data\raw\fxempire_data\Has_latest_data_available"


    def start_requests(self):
        # Read the data from the Excel file
        query_data = pd.read_excel(self.data_to_get_xlsx, sheet_name=0)

        # Iterate over each row in the DataFrame
        for index, row in query_data.iterrows():
            # Extract the 'names' value
            name = row['names']
            latest_available = row['Updating']

            # Construct the URL
            url = f"https://www.fxempire.com/api/v1/en/macro-indicators/india/{name}/history?latest=12&frequency=Daily"

            # Yield a request for each name
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={'name': name,
                      'latest_available' : latest_available}  # You can pass additional metadata if needed
            )

    def parse(self, response):

        # Retrieve 'name' from response meta
        name = response.meta.get('name')

        # Load JSON data
        data = json.loads(response.body)

        # Extract dates and close values from the data, storing them in a list
        records = []
        for item in data:
            formatted_date_str = item["formattedDate"]
            close_value = item["close"]

            # Convert the formatted date string to a datetime object
            # %b: Month name (e.g. "Apr")
            # %d: Day of the month (e.g. "05")
            # %y: 2-digit year (e.g. "24" -> 2024)
            date_obj = datetime.strptime(formatted_date_str, "%b %d, %y")
            formatted_date_iso = date_obj.strftime("%Y-%m-%d")

            records.append({
                "date": formatted_date_iso,
                "python_date" : date_obj,
                "value": close_value
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
            df = pd.read_csv(self.CSV_FILE)
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
            new_row = pd.DataFrame({"names": [name], "last_date": [latest_date_str]})
            df = pd.concat([df, new_row], ignore_index=True)
            self.logger.info(f"Added new entry for {name} with last_date {latest_date_str}")

        # Write the updated DataFrame back to the CSV file
        df.to_csv(self.CSV_FILE, index=False)

        # Create a new dictionary of records containing only entries with dates greater than the stored date.
        filtered_records = {}
        for record in records:
            # If no stored_date exists, then include all records.
            if stored_date_obj is None or record["python_date"] > stored_date_obj:
                # Use a consistent string format for the dictionary key.
                filtered_records.append({
                "date": record["date"],
                "value": record["value"]
            })

        # # Build the result dictionary with the expected schema
        # # Format: { name: [ { "python_date": datetime_object, "value": close_value }, ... ] }
        # result = {name: records}

        if response.meta.get('latest_available') == 1:
            creating_path  = f'{self.json_file_path_for_Has_latest_data_available}/{name}.json'

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

        else:
            creating_path  = f'{self.json_file_path_for_data_has_stopped_updating}/{name}.json'

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


