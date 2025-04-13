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

class InvestingComUsBondsDataSpider(scrapy.Spider):
    name = "investing_com_us_bonds_data"
    allowed_domains = ["in.investing.com"]
    start_urls = ["https://in.investing.com/rates-bonds/americas-government-bonds"]
    CSV_FILE_Six_basket_currency = "investing_com_us_bonds_data_last_date.csv"
    json_file_path_for_investing_com_us_bonds_data = r"D:\Desktop\financial_data_pipeline\data\raw\investing_com\investing_com_us_bonds_data"
    custom_settings = {
        "USER_AGENT": None,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    def start_requests(self):
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
            'Cookie': 'udid=bd2fa06ad595416f5eec9a7826034feb; adBlockerNewUserDomains=1721457081; g_state={"i_l":0}; __eventn_id=bd2fa06ad595416f5eec9a7826034feb; __eventn_uid=243780774; __eventn_id_usr=%7B%22adFreeUser%22%3A0%2C%22investingProUser%22%3A0%2C%22investingProPremiumUser%22%3A0%7D; r_p_s_n=1; SideBlockUser=a%3A2%3A%7Bs%3A10%3A%22stack_size%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Bi%3A8%3B%7Ds%3A6%3A%22stacks%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Ba%3A1%3A%7Bi%3A0%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A7%3A%221225033%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A13%3A%22%2Frates-bonds%2F%22%3B%7D%7D%7D%7D; invab=mwebd_0|noadnews_0|ovpromo_1|regwall_1; ses_num=11; last_smd=bd2fa06ad595416f5eec9a7826034feb-1744542983; identify_sent=243780774|1744631077079; lifetime_page_view_count=109; ses_id=YC5lJDQ7PjY%2Bejw6YDFiZD9tPmI0Ozs5ZWM3Mzo5YHZlcTY4MWY0cjc4PnBubWV5Y2Q3MWE%2BZ2c1NGBrOzwwZGBsZTE0ZD5rPm88NWA7YjI%2FaT5sNDE7P2UxNzI6PGBqZWE2ZDE2NGI3Yz43bmdlYmNxNythJWd2NWdgMDt6MHdgb2UkNGQ%2BZT4%2BPGJgZmJkP24%2BMzRgO2xlYzc2Oj1geGUu; upa=eyJpbnZfcHJvX2Z1bm5lbCI6IiIsIm1haW5fYWMiOiI4IiwibWFpbl9zZWdtZW50IjoiMiIsImRpc3BsYXlfcmZtIjoiMTEyIiwiYWZmaW5pdHlfc2NvcmVfYWNfZXF1aXRpZXMiOiIyIiwiYWZmaW5pdHlfc2NvcmVfYWNfY3J5cHRvY3VycmVuY2llcyI6IjEiLCJhZmZpbml0eV9zY29yZV9hY19jdXJyZW5jaWVzIjoiMSIsImFjdGl2ZV9vbl9pb3NfYXBwIjoiMCIsImFjdGl2ZV9vbl9hbmRyb2lkX2FwcCI6IjAiLCJhY3RpdmVfb25fd2ViIjoiMSIsImludl9wcm9fdXNlcl9zY29yZSI6IjAifQ%3D%3D; PHPSESSID=e667t8qu9uib96o5ovuc0k8rnd; comment_notification_243780774=1; geoC=IN; Adsfree_conversion_score=3; browser-session-counted=true; user-browser-sessions=14; adsFreeSalePopUp72aa13bb695c5357327113d1b6602174=1; gtmFired=OK; nyxDorf=Y2Q0Z2MwZCZmMjkwN2A0KDdnZDkyPTcrMTFnbDA1; smd=bd2fa06ad595416f5eec9a7826034feb-1744561167; __cf_bm=7Y.dmgBaAtpdv.2QXcvpgrP.uLcsgBN7cDvzmO9PefI-1744561168-1.0.1.1-oH_RkilJQ_jxTdP9Ub9vG9dAjd31E6j8bBFRK8h.J7IV2b6FyA7IkMYDs_W008PXsE8node80gkx12AqWn.YgMb_prl6p7dHh9MghaZmGX9rGdNedCIwp5IttyqUorRz; __cflb=0H28vS6A9ipBeEbgf2sHmZKfA5h8RM4f2oeDnPULpyh; page_view_count=1; _imntz_error=0; cf_clearance=IZUYXcNE8N7Z93qvhleDwvDXY9FG_IMqiGMHhs3dX4E-1744561170-1.2.1.1-J6eSyIfZuw6wsP367ywd0hoIOZFybzI6tMzk1Kzvdh5f4vgFLAJEgmCWkH3INyjtkpv6iT2SbSp6MeBApMvuiQctdT02sXquTGbUFA1ev0Q8WxpdS.9WLkm78SWNYRo7pVEqEEYh14ley0d2Pzywv7N6Sh5fzlkD42MiMnmsvl3nBtObU0Qrvefr7kWA7e47n5RS9PaZLQkSwcnZQzzgAIE4ozZ0vLjlguPl6iuD8vME9hO9EOEld5tTuD8ZXC9mVM8xytySaXamvfRNXg7d1gvqeMjfZbWPAz.VdHR3ePoaLlhy0Yi2MnPtbi_wF.k2u9Ekt2g_dS9zBXM6qr9RoRKeTDyb.g4YTQa.fsxQYf4'
        }

        yield scrapy.Request(
            self.start_urls[0],
            headers=headers,
            callback=self.parse,
            dont_filter=True,
            meta={"impersonate": "chrome116"},
        )

    def parse(self, response):
        # Generate dynamic start and end times
        today = datetime.today()
        start_time = (today - timedelta(days=364)).strftime("%Y-%m-%d")
        # Append " 23:59" for the end time as in your original URL
        end_time = today.strftime("%Y-%m-%d")

        # Extract all the 'pair' ids
        pair_ids = response.xpath("//*[@id='bond_table_1']//tr[starts-with(@id, 'pair')]/@id").getall()
        names = response.xpath("//*[@id='bond_table_1']//tr[starts-with(@id, 'pair')]//a/@title").getall()
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
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        }

        params = {
            'start-date': start_time,
            'end-date': end_time,
            'time-frame': 'Daily',
            'add-missing-rows': 'false',
        }

        # Build the URL with query string using urlencode
        query_string = urlencode(params)

        print(id_numbers)
        print(names)

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

        creating_path = rf'{self.json_file_path_for_investing_com_us_bonds_data}\{name}.json'

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