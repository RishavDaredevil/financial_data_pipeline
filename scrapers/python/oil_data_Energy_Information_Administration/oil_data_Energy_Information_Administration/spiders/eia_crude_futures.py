# Weekly release date: April 9, 2025   was the last update when i started
import sys

# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import scrapy
import json
import os
import pandas as pd
# Use date object for comparisons, datetime for strptime
from datetime import datetime, date, timedelta
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
from itemadapter import ItemAdapter
from scrapy.exceptions import CloseSpider
# NOTE: Pipeline class ProductProcessJsonLinesPipeline assumed to be defined
# elsewhere (e.g., in pipelines.py) and handles the file writing based on
# product-name and process-name.


# --- Define the Futures Spider ---
class EiaCrudeFuturesSpider(scrapy.Spider): # Changed class name
    # --- Essential changes ---
    name = 'eia_crude_futures' # Changed spider name
    base_url = "https://api.eia.gov/v2/petroleum/pri/fut/data" # Changed API endpoint path
    # Target series for Futures Contracts (e.g., WTI CL1, CL2, CL3, CL4)
    target_series = ["RCLC1", "RCLC2", "RCLC3", "RCLC4"] # Changed target series
    # Use a separate CSV file for futures date tracking
    start_date_csv = "eia_crude_futures_last_date.csv" # Changed CSV filename
    # --- End of essential changes ---

    allowed_domains = ['api.eia.gov']

    # --- Configuration (remains similar) ---
    api_key = "dnuwRC8pLs23bhntTeeoGX6o9wdwRETJEyUFKrO7"
    frequency = "daily"
    data_columns = ["value"]
    rows_per_page = 5000
    latest_date_scraped = None  # Will store the latest datetime.date object found

    # --- Enable the Item Pipeline ---
    # Assuming the same pipeline works for futures data structure
    custom_settings = {
        'ITEM_PIPELINES': {
            'oil_data_Energy_Information_Administration.pipelines.FuturesProductProcessJsonLinesPipeline': 1,
        },
        # 'LOG_LEVEL': 'INFO',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Read date using the specific CSV for this spider
        self.start_date_param = self._read_start_date()

    def _read_start_date(self):
        """
        Reads the start date from this spider's specific CSV file.
        Compares with calculated date to decide whether to run or exit.
        """
        try:
            today = date.today() # Use date object for today
            # --- Assumption Check ---
            # This logic assumes new data is available if run >9 days after last data.
            # Futures data might update daily (on trading days).
            # Consider adjusting timedelta based on actual futures update frequency if needed.
            last_date_spider_run_check = today - timedelta(days=11)
            # --- End Assumption Check ---

            if os.path.exists(self.start_date_csv):
                self.logger.debug(f"Reading CSV: {self.start_date_csv}")
                df = pd.read_csv(self.start_date_csv)
                if df.empty:
                    self.logger.warning(f"CSV file '{self.start_date_csv}' is empty. No start date filter applied.")
                    return None
                # Assuming date is in first row, first column
                last_saved_date_str = df.iloc[0, 0]
                last_saved_date = datetime.strptime(str(last_saved_date_str), "%Y-%m-%d").date()  # Read and convert to date object

                self.logger.debug(f"Check date: {last_date_spider_run_check}, Saved date: {last_saved_date}")

                # If saved date is same or newer than check date, assume no new data
                if last_date_spider_run_check <= last_saved_date:
                    msg = f"'{self.start_date_csv}' date ({last_saved_date}) matched/exceeded check date ({last_date_spider_run_check}). Assuming no new data."
                    self.logger.info(msg)
                    print(msg) # Also print to console
                    sys.exit(f"Stopping spider: {msg}") # Use sys.exit as per previous code
                else:
                    # Use day *after* last saved date as API start (inclusive)
                    start_date_for_api = last_saved_date + timedelta(days=1)
                    formatted_date = start_date_for_api.strftime('%Y-%m-%d')
                    self.logger.info(f"Using start date from CSV ({last_saved_date}) + 1 day: {formatted_date}")
                    return formatted_date
            else:
                # File doesn't exist, run without start date filter (first run)
                self.logger.warning(f"CSV file '{self.start_date_csv}' not found. No start date filter applied (first run?).")
                return None

        # Handle specific expected errors during file processing
        except FileNotFoundError:
             self.logger.warning(f"CSV file '{self.start_date_csv}' not found during pandas read. No start date filter applied.")
        except pd.errors.EmptyDataError:
             self.logger.warning(f"CSV file '{self.start_date_csv}' is empty (pandas check). No start date filter applied.")
        except IndexError:
             self.logger.error(f"CSV file '{self.start_date_csv}' seems malformed (IndexError). Could not read date. No start date filter applied.")
        except ValueError as e:
             self.logger.error(f"Error parsing date from CSV '{self.start_date_csv}': {e}. Check format (YYYY-MM-DD). No start date filter applied.")
        # Catch any other unexpected error during the process
        except Exception as e:
             self.logger.error(f"Unexpected error in _read_start_date for '{self.start_date_csv}': {e}. No start date filter applied.")

        return None # Return None if date couldn't be read/parsed or file not found


    def start_requests(self):
        """
        Initiates the first request to the Futures API.
        """
        params = {
            'api_key': self.api_key,
            'frequency': self.frequency,
            'data[]': self.data_columns,
            'facets[series][]': self.target_series, # Uses futures series IDs now
            'length': self.rows_per_page,
            'offset': 0
            # Optional: Add sorting if needed, e.g., latest first
            # 'sort[0][column]': 'period',
            # 'sort[0][direction]': 'desc',
        }
        if self.start_date_param:
             params['start'] = self.start_date_param

        # Uses the futures base_url now
        start_url = f"{self.base_url}?{urlencode(params, doseq=True)}"
        self.logger.info(f"Starting request: {start_url}")
        yield scrapy.Request(
            url=start_url,
            callback=self.parse,
            meta={'offset': 0, 'length': self.rows_per_page}
        )

    def parse(self, response):
        """
        Parses the JSON response, tracks latest date, yields items, handles pagination.
        Updates the specific CSV for *this* spider upon completion.
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

        # Track latest date and yield items
        for record in records:
            period_str = record.get('period')
            if period_str:
                try:
                    current_item_date = datetime.strptime(period_str, '%Y-%m-%d').date()
                    if self.latest_date_scraped is None or current_item_date > self.latest_date_scraped:
                        self.latest_date_scraped = current_item_date
                except ValueError:
                    self.logger.warning(f"Could not parse date from period '{period_str}' in record. Skipping date check.")
                except Exception as e:
                    self.logger.error(f"Unexpected error parsing period date '{period_str}': {e}")
            yield record # Yield item for the pipeline

        # --- Pagination Logic ---
        current_offset = response.meta.get('offset', 0)
        try:
            total_records_int = int(total_records) if total_records is not None else None
        except (ValueError, TypeError):
             self.logger.error(f"Could not convert 'total' ({total_records}) to integer. Stopping pagination.")
             total_records_int = None # Ensure pagination stops if total is invalid

        if total_records_int is not None:
            next_offset = current_offset + len(records)
            self.logger.info(f"Page fetched: Offset={current_offset}, Received={len(records)}, Total={total_records_int} (for query)")

            if next_offset < total_records_int and len(records) > 0:
                # Construct URL for the next page
                next_page_params = {
                    'api_key': self.api_key,
                    'frequency': self.frequency,
                    'data[]': self.data_columns,
                    'facets[series][]': self.target_series, # Futures series
                    'length': str(self.rows_per_page),
                    'offset': str(next_offset)
                    # Optional: Add sorting params if used in initial request
                    # 'sort[0][column]': 'period',
                    # 'sort[0][direction]': 'desc',
                }
                parsed_url = urlparse(response.url)
                next_page_query = urlencode(next_page_params, doseq=True)
                # Uses self.base_url which is now the futures URL
                next_page_url = urlunparse(
                    (parsed_url.scheme, parsed_url.netloc, parsed_url.path,
                     '', next_page_query, '')
                )
                self.logger.info(f"Requesting next page: {next_page_url}")
                yield scrapy.Request(
                    url=next_page_url,
                    callback=self.parse,
                    meta={'offset': next_offset, 'length': self.rows_per_page}
                )
            else:
                 # This is the end of pagination for this run
                 self.logger.info("No more pages to fetch for this query.")
                 # --- Update this spider's specific CSV with the latest date found ---
                 if self.latest_date_scraped:
                     try:
                        self.logger.info(f"Updating '{self.start_date_csv}' with latest date found: {self.latest_date_scraped}")
                        # Use pandas to overwrite the CSV with the new latest date
                        df = pd.DataFrame([[self.latest_date_scraped.strftime('%Y-%m-%d')]], columns=["date"]) # Ensure correct header and format
                        df.to_csv(self.start_date_csv, index=False)
                        self.logger.info(f"Successfully updated '{self.start_date_csv}'")
                     except IOError as e:
                         self.logger.error(f"Could not write latest date to CSV file '{self.start_date_csv}': {e}")
                     except Exception as e:
                         self.logger.error(f"Unexpected error updating CSV '{self.start_date_csv}': {e}")

                 else: # Log if no date was tracked
                      self.logger.info("No data periods were parsed in this run to update the CSV.")

        else:
            # If total_records is None (e.g., not present in response), stop pagination.
            self.logger.warning(f"Cannot determine total records from API response. Stopping pagination.")


# --- How to Run ---
# 1. Save the code above as a Python file (e.g., eia_futures_spider.py).
# 2. Create/manage the CSV file named `eia_crude_futures_last_date.csv`.
#    It needs a header `date` and the last known data date (YYYY-MM-DD) below it.
# 3. Ensure the pipeline (`ProductProcessJsonLinesPipeline`) is accessible.
# 4. Run the spider:
#    scrapy runspider eia_futures_spider.py