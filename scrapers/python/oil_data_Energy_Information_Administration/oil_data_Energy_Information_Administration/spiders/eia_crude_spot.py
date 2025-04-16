# Weekly release date: April 9, 2025   was the last update when i started
import sys

# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import scrapy
import json
import os
import pandas as pd
from datetime import datetime, timedelta  # Import datetime for date handling
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
from itemadapter import ItemAdapter
from scrapy.exceptions import CloseSpider


# --- Define the Spider ---
class EiaCrudeSpotSpider(scrapy.Spider):
    name = 'eia_crude_spot'
    allowed_domains = ['api.eia.gov']

    # --- Configuration ---
    api_key = "dnuwRC8pLs23bhntTeeoGX6o9wdwRETJEyUFKrO7"
    base_url = "https://api.eia.gov/v2/petroleum/pri/spt/data"
    frequency = "daily"
    data_columns = ["value"]
    rows_per_page = 5000
    target_series = ["RWTC", "RBRTE"]
    start_date_csv = "eia_crude_spot_last_date.csv" # Name of the CSV file
    latest_date_scraped = None  # Will store the latest datetime.date object found

    # --- Enable the *Correct* Item Pipeline ---
    custom_settings = {
        'ITEM_PIPELINES': {
            # Make sure the class name matches the modified pipeline
             'oil_data_Energy_Information_Administration.pipelines.SpotProductProcessJsonLinesPipeline': 1,
        },
        # Optional: Configure logging level if needed
        # 'LOG_LEVEL': 'INFO',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs) # Important for Scrapy initialization
        self.start_date_param = self._read_start_date() # Read date during initialization

    def _read_start_date(self):
        """Reads the start date from the CSV file."""
        try:
            today = datetime.today()
            last_date_spider_run = (today - timedelta(days=9)).date() # Weekly release date: April 9, 2025   was the last update when i started

            if os.path.exists(self.start_date_csv):
                df = pd.read_csv(self.start_date_csv)
                last_saved_date = datetime.strptime(df.iloc[0, 0], "%Y-%m-%d").date()  # Read and convert to date

                if last_date_spider_run <= last_saved_date:
                    self.log(last_date_spider_run)
                    print(last_date_spider_run)
                    sys.exit("CSV file date matched New data yet to be uploaded.")
                else:
                    # Format it back to string just in case, though not strictly necessary here
                    formatted_date = last_saved_date.strftime('%Y-%m-%d')
                    self.logger.info(f"Using start date from CSV: {formatted_date}")
                    return formatted_date

        except (FileNotFoundError): # Should be caught by os.path.exists, but belt-and-suspenders
             self.logger.warning(f"CSV file '{self.start_date_csv}' not found during pandas read. No start date filter applied.")
             return None
        except (pd.errors.EmptyDataError):
             self.logger.warning(f"CSV file '{self.start_date_csv}' is empty (pandas check). No start date filter applied.")
             return None
        except (IndexError):
             self.logger.error(f"CSV file '{self.start_date_csv}' seems malformed (IndexError). Could not read date. No start date filter applied.")
             return None
        except (ValueError) as e:
             self.logger.error(f"Error parsing date from CSV '{self.start_date_csv}': {e}. Check format (YYYY-MM-DD). No start date filter applied.")
             return None
        return None # Return None if date couldn't be read/parsed

    def start_requests(self):
        """
        Initiates the first request to the API, incorporating the start date if available.
        """
        params = {
            'api_key': self.api_key,
            'frequency': self.frequency,
            'data[]': self.data_columns,
            'facets[series][]': self.target_series,
            'length': self.rows_per_page,
            'offset': 0
        }
        # Add start date parameter ONLY if it was successfully read and formatted
        if self.start_date_param:
             params['start'] = self.start_date_param

        start_url = f"{self.base_url}?{urlencode(params, doseq=True)}"
        self.logger.info(f"Starting request: {start_url}")
        yield scrapy.Request(
            url=start_url,
            callback=self.parse,
            # Meta doesn't need start date, pagination handles offsets
            meta={'offset': 0, 'length': self.rows_per_page}
        )

    def parse(self, response):
        """
        Parses the JSON response, yields items for the pipeline, handles pagination.
        Ensures 'start' param is NOT carried over in pagination.
        """
        try:
            data = json.loads(response.body)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON from {response.url}")
            return

        api_response = data.get('response', {})
        records = api_response.get('data', [])
        total_records = api_response.get('total')

        if not isinstance(records, list):
             self.logger.warning(f"Expected a list of records in 'data', but got {type(records)} from {response.url}")
             records = []

        # Yield items for the pipeline (pipeline now buffers them)
        for record in records:
            period_str = record.get('period')
            if period_str:
                try:
                    # Parse the date string into a date object
                    current_item_date = datetime.strptime(period_str, '%Y-%m-%d').date()

                    # Update latest_date_scraped if this item's date is later
                    if self.latest_date_scraped is None or current_item_date > self.latest_date_scraped:
                        self.latest_date_scraped = current_item_date
                        # Optional: log when the latest date is updated
                        # self.logger.debug(f"Updated latest date scraped to: {self.latest_date_scraped}")

                except ValueError:
                    self.logger.warning(
                        f"Could not parse date from period '{period_str}' in record: {record}. Skipping date check for this record.")
                except Exception as e:
                    self.logger.error(f"Unexpected error parsing period date '{period_str}': {e}")
            yield record # Yield item for the pipeline

        # --- Pagination Logic ---
        current_offset = response.meta.get('offset', 0)
        try:
            total_records_int = int(total_records) if total_records is not None else None
        except (ValueError, TypeError):
             self.logger.error(f"Could not convert 'total' ({total_records}) to integer from {response.url}")
             total_records_int = None

        if total_records_int is not None:
            next_offset = current_offset + len(records)
            self.logger.info(f"Page fetched: Offset={current_offset}, Received={len(records)}, Total={total_records_int} (for query)")

            if next_offset < total_records_int and len(records) > 0:
                # Construct URL for the next page WITHOUT the 'start' parameter
                # Base parameters needed for subsequent pages
                next_page_params = {
                    'api_key': self.api_key,
                    'frequency': self.frequency,
                    'data[]': self.data_columns,
                    'facets[series][]': self.target_series,
                    'length': str(self.rows_per_page), # Keep length
                    'offset': str(next_offset)        # Set NEW offset
                }

                # Rebuild the URL using only necessary parameters
                # We parse the original URL only to get the base path easily
                parsed_url = urlparse(response.url)
                next_page_query = urlencode(next_page_params, doseq=True)
                next_page_url = urlunparse(
                    (parsed_url.scheme, parsed_url.netloc, parsed_url.path,
                     '', next_page_query, '') # Keep params, query; clear others
                )

                self.logger.info(f"Requesting next page: {next_page_url}")
                yield scrapy.Request(
                    url=next_page_url,
                    callback=self.parse,
                    # Pass new offset for the next iteration
                    meta={'offset': next_offset, 'length': self.rows_per_page}
                )
            else:
                 self.logger.info("No more pages to fetch for this query.")
                 # --- Log the latest date found ---
                 if self.latest_date_scraped:
                     # If new date, update CSV and proceed with scraping
                     df = pd.DataFrame([[self.latest_date_scraped]], columns=["date"])
                     df.to_csv(self.start_date_csv, index=False)
                     self.logger.info(
                         f"Latest date scraped in this run: {self.latest_date_scraped.strftime('%Y-%m-%d')}")

        else:
            self.logger.warning(f"Cannot determine total records from {response.url}. Stopping pagination.")


# --- How to Run ---
# 1. Save the code above as a Python file (e.g., eia_crude_spider.py).
# 2. Create a CSV file named `start_date.csv` in the same directory:
#    StartDate
#    2024-01-15
#    (Replace the date with your desired start date in YYYY-MM-DD format)
# 3. Make sure you have Scrapy installed (`pip install scrapy`).
# 4. Open your terminal or command prompt.
# 5. Navigate to the directory where you saved the files.
# 6. Run the spider using the command:
#    scrapy runspider eia_crude_spider.py
#
# The spider will now:
# - Read the start date from `start_date.csv`.
# - Make the initial API request including the `start` parameter (if valid).
# - Fetch data page by page (subsequent requests will NOT include `start`).
# - Buffer the fetched items.
# - At the end, sort the items collected *in this run* by date for each series.
# - Append the sorted items to the respective `.jsonl` files:
#   - `D:\Desktop\financial_data_pipeline\data\raw\Oil data\oil_data_Energy_Information_Administration\wti_crude_spot_prices.jsonl`
#   - `D:\Desktop\financial_data_pipeline\data\raw\Oil data\oil_data_Energy_Information_Administration\brent_crude_spot_prices.jsonl`