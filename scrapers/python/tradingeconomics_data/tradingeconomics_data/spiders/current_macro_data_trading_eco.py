# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import os
import re

import scrapy
import json
from datetime import datetime
from dateutil.utils import today
from tradingeconomics_data.helper_func import decode_obfuscated_data
import pandas as pd # Not needed if only using for data structure
from scrapy.exceptions import CloseSpider
from rich import print # Keep if you use rich print elsewhere, otherwise standard logging is preferred


class CurrentMacroDataTradingEcoSpider(scrapy.Spider):
    name = "current_macro_data_trading_eco"
    allowed_domains = ["tradingeconomics.com"]
    start_urls = ["https://tradingeconomics.com/india/indicators"]
    CSV_FILE_current_macro_data_trading_eco = "current_macro_data_trading_eco_last_date.csv"
    json_file_path_for_current_macro_data_trading_eco = r"D:\Desktop\financial_data_pipeline\data\raw\trading_economics\current_macro_data_trading_eco"


    def parse(self, response):
        # # Generate dynamic start and end times
        # today = datetime.today()
        # start_time = (today - timedelta(days=364)).strftime("%Y-%m-%d")
        # # Append " 23:59" for the end time as in your original URL
        # end_time = today.strftime("%Y-%m-%d")

        # Extract all the 'pair' ids
        pair_ids = response.xpath("//tr[starts-with(@id, 'pair')]/@id").getall()
        names = response.xpath("//tr[starts-with(@id, 'pair')]//a[starts-with(@title, 'Ind')]/@title").getall()
        # Remove the "pair_" prefix to get only the numeric part
        id_numbers = [pid.replace("pair_", "") for pid in pair_ids]

        urls_a_elmnt = response.xpath('//*[@id="ctl00_ContentPlaceHolder1_ctl00_Panel1"]/div/div/div/div/div/table/tbody/tr/td[1]/a')

        urls = urls_a_elmnt.xpath('./@href').getall()
        names = urls_a_elmnt.xpath('normalize-space(./text())').getall()

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'referer': 'https://tradingeconomics.com/india/indicators',
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'Cookie': '__zlcmid=1QbnSE622oXk4Gy; ASP.NET_SessionId=wyqeo5fh2p45t4ueqaxr2hdt; TEServer=TEIIS2'
        }

        # Now id_numbers is a list of strings like ['24004', ...]
        # You can now loop over these to make your Scrapy requests, e.g.:
        for name,url in zip(names,urls):
            url = f'https://tradingeconomics.com{url}'
            yield scrapy.Request(
                url,
                headers=headers,
                callback=self.parse_each_url,
                # dont_filter=True,
                # meta={"impersonate": "chrome116"},
                cb_kwargs={"name": name}
            )


    def parse_each_url(self, response, name):
        found_TEChartsToken = False
        found_TESymbol = False
        found_TELastUpdate = False
        found_TEObfuscationkey = False

        script_tags = response.xpath('//script[contains(text(), "TESymbol")]/text()').getall()

        for script_content in script_tags:
            # Use regex to extract the value assigned to TESymbol from the script content
            match = re.search(r"var\s+TESymbol\s*=\s*'([^']+)'", script_content)
            if match:
                tesymbol_value = match.group(1).strip()
                # Check if the extracted value is not blank (non-empty)
                if tesymbol_value.strip():
                    self.logger.info("Extracted TESymbol: %s", tesymbol_value)
                    # yield {'TESymbol': tesymbol_value}
                    found_TESymbol = True
                    # If you only need one valid result, you can break here.
                    break
        else:
            # No valid TESymbol found with a non-empty value
            self.logger.error("No script tag contained a non-empty TESymbol value.")


        # Select all script tags that contain the term "TEChartsToken"
        script_tags = response.xpath('//script[contains(text(), "TEChartsToken")]/text()').getall()

        for script in script_tags:
            # Apply regex to extract the token value from each script tag
            token_match = re.search(r"var\s+TEChartsToken\s*=\s*'([^']+)'", script)
            if token_match:
                token_value = token_match.group(1).strip()
                # Check if the token value is non-empty
                if token_value.strip():
                    self.logger.info("Extracted TEChartsToken: %s", token_value)
#                     yield {'TEChartsToken': token_value}
                    found_TEChartsToken = True
                    # If you expect a single valid result, break out of the loop.
                    break
        else:
            self.logger.error("No non-empty TEChartsToken value was found.")

        # Extract all script tags that contain the "TELastUpdate" string
        script_tags = response.xpath('//script[contains(text(), "TELastUpdate")]/text()').getall()

        for script in script_tags:
            # Look for the TELastUpdate assignment using a regular expression
            match = re.search(r"TELastUpdate\s*=\s*'([^']+)'", script)
            if match:
                telastupdate_value = match.group(1).strip()
                # Check if the value is non-empty
                if telastupdate_value.strip():
                    self.logger.info("Extracted TELastUpdate: %s", telastupdate_value)
#                     yield {'TELastUpdate': telastupdate_value}
                    found_TELastUpdate = True
                    # If you only need one valid result, break the loop
                    break
        else:
            self.logger.error("No non-empty TELastUpdate value found.")

        if found_TEChartsToken and found_TESymbol:
            yield {"name": name,'TESymbol': tesymbol_value, 'TELastUpdate': telastupdate_value,'TEChartsToken': token_value}
            try:
                url = f'https://d3ii0wo49og5mi.cloudfront.net/economics/{tesymbol_value.strip()}?&span=10y&v={telastupdate_value.strip()}&key={token_value.strip()}'
                headers = {
                    'accept': 'application/json, text/javascript, */*; q=0.01',
                    'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
                    'cache-control': 'no-cache',
                    'origin': 'https://tradingeconomics.com',
                    'pragma': 'no-cache',
                    'priority': 'u=1, i',
                    'referer': 'https://tradingeconomics.com/',
                    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'cross-site',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
                }
                # Select all script tags containing the "TEObfuscationkey" identifier
                script_tags = response.xpath('//script[contains(text(), "TEObfuscationkey")]/text()').getall()

                for script in script_tags:
                    # Use a regex to extract the TEObfuscationkey value
                    match = re.search(r"var\s+TEObfuscationkey\s*=\s*'([^']+)'", script)
                    if match:
                        key_value = match.group(1).strip()
                        if key_value.strip():
                            self.logger.info("Extracted TEObfuscationkey: %s", key_value)
                            yield {'TEObfuscationkey': key_value}
                            found_TEObfuscationkey = True
                            break  # Stop after finding the first valid non-empty value.
                else:
                    self.logger.error("No valid TEObfuscationkey value found.")

                self.logger.debug(
                    f"Checking found_TEObfuscationkey for {name}. Value: {found_TEObfuscationkey}")  # Add this

                if found_TEObfuscationkey:
                    yield scrapy.Request(
                        url,
                        headers=headers,
                        callback=self.parse_decode_parse,
                        dont_filter=True,
                        # meta={"impersonate": "chrome116"},
                        cb_kwargs={"name": name,"key" : key_value.strip()}
                    )
            except Exception as e:
                print("request failed may be different URL str for",name, e)

    def parse_decode_parse(self, response, name, key):

        resp = decode_obfuscated_data(response.text,key)

        json_data = json.loads(resp)

        # Navigate the nested structure:
        # Our JSON is a list with one element. Inside, the key 'series' is a list,
        # inside that each element is a dictionary with key 'serie'
        # 'serie' has a key 'data' which is the list of data points we need.
        if json_data:
            try:
                series_data = json_data[0]['series'][0]['serie']['data']
            except (IndexError, KeyError) as e:
                self.logger.error("Error extracting data: %s", e)
        else:
            self.logger.error("Empty JSON response")

        records = []

        for item in series_data:
            # Extract the required fields
            formatted_date_str = item[3]
            value = item[0]

            # Convert the formatted date string ("Mar 15, 2021") to a datetime object.
            # The format specifier "%b %d, %Y" corresponds to "Mar 15, 2021"
            date_obj = datetime.strptime(formatted_date_str, "%Y-%m-%d")
            formatted_date_iso = date_obj.strftime("%Y-%m-%d")

            records.append({
                "date": formatted_date_iso,
                "python_date": date_obj,
                "Value": value,
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
            df = pd.read_csv(self.CSV_FILE_current_macro_data_trading_eco)
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
        df.to_csv(self.CSV_FILE_current_macro_data_trading_eco, index=False)

        # Create a new dictionary of records containing only entries with dates greater than the stored date.
        filtered_records = []
        for record in records:
            # If no stored_date exists, then include all records.
            if stored_date_obj is None or record["python_date"] > stored_date_obj:
                # Use a consistent string format for the dictionary key.
                filtered_records.append({
                    "date": record["date"],
                    "Value": record["Value"],
                })

        creating_path = rf'{self.json_file_path_for_current_macro_data_trading_eco}\{name}.json'

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