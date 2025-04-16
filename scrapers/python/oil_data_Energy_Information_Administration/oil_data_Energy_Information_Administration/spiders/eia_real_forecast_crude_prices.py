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


class EiaRealForecastCrudePricesSpider(scrapy.Spider):
    name = "eia_real_forecast_crude_prices"
    allowed_domains = ["www.eia.gov"]
    start_urls = ["https://www.eia.gov/outlooks/steo/realprices/data/index.php?type=getData&periodType=M&formulaList=RAIMUUS%2CRAIMUUS_RP"]
    start_date_csv = "eia_real_forecast_crude_prices_last_date.csv"
    json_file_path_for_eia_real_crude_prices = r"D:\Desktop\financial_data_pipeline\data\raw\Oil data\oil_data_Energy_Information_Administration\eia_real_crude_prices.jsonl"
    json_file_path_for_eia_forecast_crude_prices = r"D:\Desktop\financial_data_pipeline\data\raw\Oil data\oil_data_Energy_Information_Administration\eia_forecast_crude_prices.jsonl"

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
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Referer': 'https://www.eia.gov/outlooks/steo/realprices/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Cookie': 'ak_bmsc=0A7568E39682B7371F56CB5CA48F3A2B~000000000000000000000000000000~YAAQF3k1F8KGORuWAQAAFDHvPRvxEvegCP7Fq4fjRUG+fVQ1nRPnGShZbMcOHUQ2334Qtywx8C/GS5RMRmc6FodcscGIVMMm2K2EFYRZ/ota6iUlzhwyN5gvxk1WnhZc0rSzLPQF2iq0ujQsX8n9Lbrq8EixF2gwEJRx7i/O+qfSCk3RIU3hsJFXarl0dOoA4lKtrb6uAn6IOiUpy94RXY3EKXIkfqX+jai9ZdjrciUKftrmOTCU+WFUD8xWRRIAATkVVHu+JXDTv/DZFDJ5bIk525MmCYgbTnLB/PjkttaFbMHlxcrkpy5OxCQSBbRJc9gQL6VygYpow2ogFicuWMAQvHlVhbo/8bgJRKFabROjzFQZfbA905XE; bm_mi=76516AB8208C23AC6874EBC2B2474353~YAAQTHxBF0sIAQuWAQAADfATPhvixoAtjjBoUBPGKbV4Aujw4I/Ew7FWGjWunl+giPbtdi0FRhFDYSUFnE9JapnSPxnbv3uKQN3s9uGs28E2jB0F1+H2VccvZA1cq1fV5I2tc25InpowfKPgdmkzwQ0t1rLaOOwDmdDFIsBjHtsuJ1r18vIGAQ4kU7AUKZO8YAFM04C0MSpAnHpEo9MeAu6D8acmK6GIW6jAZqiZoH4WHwvv2YzKDAJ6wHharrmve2vUosYQ21TfkQToee35gWFSBeoji6GzxibtrK6QN5U0JWh5xFdQbI+XUa50KHCg7FBOg+niFWUyKF29Yg==~1; bm_sv=754ED014BF849B6280C2C6B6C1AF79A1~YAAQTHxBFzsJAQuWAQAAHRwUPhvl+PqudLnLbkd4Qd4BYOviVeenMawcj9Tr7+SAGkl/QVU2nVR3BuiN26YKD8zWj/dOvwdJZT4qW8pkw3C4/8mZrxwVnl0AykcvpTUf5MODlghXdpxmwKeJHOIOilCzFOnYN0SnVzmPI108s5nCHJ0xbh8l2M7WPkaxj1z8bT/zbe8MCu7mfiZoOIbGWFpQXNnrkUDf+4r5kmI/+18EwQq/D4957kLm/OVizQ==~1; ak_bmsc=0A7568E39682B7371F56CB5CA48F3A2B~000000000000000000000000000000~YAAQTHxBF7ARAQuWAQAAA34VPhtysQbrkOzG09vaDOLuW8htASiEFsRButlpIbxf3hEridk7wh7WBhq5FTMlwnT7PSFTDMAXoPGJ3Gro1s5odsfpPPCHUIBKBCFVcChWe0ZocgmixbbLnDHQpgfKHwBrO6N78LYQSecv/BjFDgJ4+Ft8fQCD69afUAX5pWY4rMmvWKxIkniuR651SIwwJ4HK7LjUahQjd3GaGRZ6rlkW/PDLTSw73ZLom2ZCYJdk2h9CDiPD8PzBBQHMyGCjxWp7ijSfqyNnPUmstejIT5sdLGcwaP5b+6ijwfB6MwffZk/56MH+ariypEFwIo1y4OObQNYEKR4R0UzBoCzxiXMpemj5vP1lCjq/nRMvnPtt8sKJOvVG2sE0Uc67AxHmCTA='
        }

        url = self.start_urls[0]
        yield scrapy.Request(
            url,
            headers=headers,
            callback=self.parse,
            dont_filter=True,
            meta={"impersonate": "chrome116"},
        )

    def parse(self, response):
        json_data = json.loads(response.body)

        past_data_records = []

        forecast_data_records = []

        data_till_date_is_historical =  datetime.strptime(str(json_data["HISTORICALDATE"]), "%Y%m")

        for item in json_data["DATA"]:
            # Extract the required fields
            formatted_date_str = str(item["DATECODE"])
            Nominal_value = item["RAIMUUS"]
            Real_value = item["RAIMUUS_RP"]

            # Convert the formatted date string ("Mar 15, 2021") to a datetime object.
            # The format specifier "%b %d, %Y" corresponds to "Mar 15, 2021"
            date_obj = datetime.strptime(formatted_date_str, "%Y%m")
            formatted_date_iso = date_obj.strftime("%Y-%m-%d")

            if date_obj <= data_till_date_is_historical:
                past_data_records.append({
                    "date": formatted_date_iso,
                    "Nominal_value": Nominal_value,
                    "Real_value": Real_value,
                    "Past_data": True
                })
            else:
                forecast_data_records.append({
                    "date": formatted_date_iso,
                    "Nominal_value": Nominal_value,
                    "Real_value": Real_value,
                    "Past_data": False
                })


        # Ensure we have at least one record
        if not past_data_records:
            self.logger.info(f"No past_data_records found")
            return

        try:
            last_date_spider_run_check = data_till_date_is_historical

            if os.path.exists(self.start_date_csv):
                self.logger.debug(f"Reading CSV: {self.start_date_csv}")
                df = pd.read_csv(self.start_date_csv)
                if df.empty:
                    self.logger.warning(f"CSV file '{self.start_date_csv}' is empty. No start date filter applied.")
                    stored_date_obj = None
                # Assuming date is in first row, first column
                last_saved_date_str = df.iloc[0, 0]
                last_saved_date = datetime.strptime(str(last_saved_date_str), "%Y-%m-%d").date()  # Read and convert to date object

                self.logger.debug(f"Check date: {last_date_spider_run_check}, Saved date: {last_saved_date}")

                # If saved date is same or newer than check date, assume no new data
                if last_date_spider_run_check == last_saved_date:
                    msg = f"'{self.start_date_csv}' date ({last_saved_date}) matched/exceeded check date ({last_date_spider_run_check}). Assuming no new data."
                    self.logger.info(msg)
                    print(msg) # Also print to console
                    raise CloseSpider(f"Stopping spider: {msg}")
                else:
                    try:
                        self.logger.info(
                            f"Updating '{self.start_date_csv}' with latest date found: {data_till_date_is_historical.date()}")
                        # Use pandas to overwrite the CSV with the new latest date
                        df = pd.DataFrame([[data_till_date_is_historical.strftime('%Y-%m-%d')]],
                                          columns=["date"])  # Ensure correct header and format
                        df.to_csv(self.start_date_csv, index=False)
                        self.logger.info(f"Successfully updated '{self.start_date_csv}'")
                    except IOError as e:
                        self.logger.error(f"Could not write latest date to CSV file '{self.start_date_csv}': {e}")
                    except Exception as e:
                        self.logger.error(f"Unexpected error updating CSV '{self.start_date_csv}': {e}")
            else:
                # File doesn't exist, run without start date filter (first run)
                self.logger.warning(f"CSV file '{self.start_date_csv}' not found. No start date filter applied (first run?).")
                stored_date_obj = None
                try:
                    self.logger.info(
                        f"Updating '{self.start_date_csv}' with latest date found: {data_till_date_is_historical.date()}")
                    # Use pandas to overwrite the CSV with the new latest date
                    df = pd.DataFrame([[data_till_date_is_historical.strftime('%Y-%m-%d')]],
                                      columns=["date"])  # Ensure correct header and format
                    df.to_csv(self.start_date_csv, index=False)
                    self.logger.info(f"Successfully updated '{self.start_date_csv}'")
                except IOError as e:
                    self.logger.error(f"Could not write latest date to CSV file '{self.start_date_csv}': {e}")
                except Exception as e:
                    self.logger.error(f"Unexpected error updating CSV '{self.start_date_csv}': {e}")

        # Handle specific expected errors during file processing
        except FileNotFoundError:
             self.logger.warning(f"CSV file '{self.start_date_csv}' not found during pandas read. No start date filter applied.")
             stored_date_obj = None
        except pd.errors.EmptyDataError:
             self.logger.warning(f"CSV file '{self.start_date_csv}' is empty (pandas check). No start date filter applied.")
             stored_date_obj = None
        except IndexError:
             self.logger.error(f"CSV file '{self.start_date_csv}' seems malformed (IndexError). Could not read date. No start date filter applied.")
             stored_date_obj = None
        except ValueError as e:
             self.logger.error(f"Error parsing date from CSV '{self.start_date_csv}': {e}. Check format (YYYY-MM-DD). No start date filter applied.")
             stored_date_obj = None
        # Catch any other unexpected error during the process
        except Exception as e:
             self.logger.error(f"Unexpected error (most probably from Closespider) in _read_start_date for '{self.start_date_csv}': {e}. No start date filter applied.")
             stored_date_obj = None

        # Create a new dictionary of past_data_records containing only entries with dates greater than the stored date.
        filtered_past_data_records = []
        for record in past_data_records:
            # If no stored_date exists, then include all past_data_records.
            if stored_date_obj is None or record["python_date"] > stored_date_obj:
                # Use a consistent string format for the dictionary key.
                filtered_past_data_records.append({
                    "date": record["date"],
                    "Nominal_value": record["Nominal_value"],
                    "Real_value": record["Real_value"],
                    "Past_data": record["Past_data"]
                })

        creating_path = r"D:\Desktop\financial_data_pipeline\data\raw\Oil data\oil_data_Energy_Information_Administration"

        with open(self.json_file_path_for_eia_real_crude_prices, "a", encoding="utf-8") as file:
            for item in filtered_past_data_records:
                json_line = json.dumps(item)
                file.write(json_line + "\n")

        with open(self.json_file_path_for_eia_forecast_crude_prices, "w", encoding="utf-8") as file:
            for item in forecast_data_records:
                json_line = json.dumps(item)
                file.write(json_line + "\n")

        print(f"Data appended successfully to {creating_path} directory")
