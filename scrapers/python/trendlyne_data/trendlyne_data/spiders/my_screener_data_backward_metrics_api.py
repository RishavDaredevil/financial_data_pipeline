import scrapy
import json
from datetime import datetime
from dateutil.utils import today
from trendlyne_data.helper_func import non_curl_cffi_req
# import pandas as pd # Not needed if only using for data structure
from scrapy.exceptions import CloseSpider
from rich import print # Keep if you use rich print elsewhere, otherwise standard logging is preferred

# Assume necessary imports like scrapy, json, datetime, today are present above the class

class MyScreenerDataBackwardMetricsApiSpider(scrapy.Spider):
    name = "my_screener_data_backward_metrics_api"
    allowed_domains = ["trendlyne.com"]
    start_urls = ["https://httpbin.org/headers"] # Keep as per original structure
    full_stock_data = {'backward_metric_1' : [], 'backward_metric_2' : []}
    unstuctured_trendlyne_data = {'backward_metric_1' : [], 'backward_metric_2' : []}
    jsonl_file_path_structured = r"D:\Desktop\financial_data_pipeline\data\raw\trendlyne_data\my_screener_data_backward_metrics_structured_init.jsonl"
    jsonl_file_path_unstuctured_trendlyne_data = r"D:\Desktop\financial_data_pipeline\data\raw\trendlyne_data\my_screener_data_backward_metrics_unstructured_init.jsonl"

    # Initialize html variables to None
    html1 = None
    html2 = None

    # Original commented-out start_requests
    # def start_requests(self):
    #     # ... (original commented code) ...
    #     pass

    def __init__(self, *args, **kwargs):
        """
        Spider constructor. Fetches data synchronously using requests
        before Scrapy's engine starts processing.
        """
        super().__init__(*args, **kwargs) # Important to call parent constructor

        print("[blue]Spider __init__: Starting synchronous data fetch...[/blue]")

        url1 = "https://trendlyne.com/fundamentals/tl-all-in-one-screener-data-get/?screenpk=661798&perPageCount=10000&groupType=all&groupName=all"
        url2 = "https://trendlyne.com/fundamentals/tl-all-in-one-screener-data-get/?screenpk=662076&perPageCount=4000&groupType=all&groupName=all"

        # --- Headers (Copied directly from your original __init__) ---
        headers1 = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://trendlyne.com/fundamentals/stock-screener/661798/backward-metrics/',
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            'Cookie': '__stripe_mid=cfbee81c-e608-4244-8148-b5fa7bfd6a3f6a2264; .trendlyne=8pgukjbte9b1su8zrr3omzm0e1sg1d5u; csrftoken=ATFMQFil066z6eYT2FivrpjVt1WbrxicCXw4wkgY8ZZJYijqkPSc278rYIqvoHx2; csrftoken=ATFMQFil066z6eYT2FivrpjVt1WbrxicCXw4wkgY8ZZJYijqkPSc278rYIqvoHx2' # Ensure cookie validity
        }

        headers2 = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://trendlyne.com/fundamentals/stock-screener/662076/backward-metrics-2/',
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            'Cookie': '__stripe_mid=cfbee81c-e608-4244-8148-b5fa7bfd6a3f6a2264; .trendlyne=8pgukjbte9b1su8zrr3omzm0e1sg1d5u; csrftoken=ATFMQFil066z6eYT2FivrpjVt1WbrxicCXw4wkgY8ZZJYijqkPSc278rYIqvoHx2; csrftoken=ATFMQFil066z6eYT2FivrpjVt1WbrxicCXw4wkgY8ZZJYijqkPSc278rYIqvoHx2'
        }

        # --- Fetch and Process Response 1 ---
        print(f"[blue]Fetching data from: {url1}[/blue]")
        response_obj_1 = non_curl_cffi_req(url1, headers=headers1)

        if response_obj_1 is not None: # Check if request itself succeeded
            print(f"[green]Received response for url1, Status Code: {response_obj_1.status_code}[/green]")
            # Basic check if content type suggests JSON (optional but helpful)
            content_type = response_obj_1.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                try:
                    # Try parsing JSON to be sure, but store the original text
                    response_obj_1.json() # This tests if it's valid JSON
                    self.html1 = response_obj_1.text
                    print("[green]Successfully stored text content for html1.[/green]")
                except json.JSONDecodeError:
                    print(f"[bold red]Error: Response for url1 (Status {response_obj_1.status_code}) is not valid JSON despite content type.[/bold red]")
                    print(f"Response snippet: {response_obj_1.text[:200]}...") # Show beginning of bad response
            else:
                 # Even if not JSON, maybe store text if status was OK? Depends on requirements.
                 # For this case, we expect JSON, so treat non-JSON content type as potential issue
                 print(f"[yellow]Warning: Unexpected content type for url1: '{content_type}'. Storing text anyway.[/yellow]")
                 # You could choose to store text even if not JSON, or treat it as error
                 self.html1 = response_obj_1.text # Store text even if content-type mismatch, relying on later parse check
                 # Alternatively: print("[bold red]Error: Expected JSON content type, but got '{content_type}'.[/bold red]")

        else:
            # non_curl_cffi_req returned None, meaning request failed (network, timeout, or status code 4xx/5xx)
            print("[bold red]Error: Request failed for url1 (non_curl_cffi_req returned None). Cannot proceed with this data.[/bold red]")
            # Decide how to handle this failure - maybe close the spider?
            # For now, self.html1 remains None

        # --- Fetch and Process Response 2 ---
        print(f"\n[blue]Fetching data from: {url2}[/blue]")
        response_obj_2 = non_curl_cffi_req(url2, headers=headers2)

        if response_obj_2 is not None: # Check if request itself succeeded
            print(f"[green]Received response for url2, Status Code: {response_obj_2.status_code}[/green]")
            content_type = response_obj_2.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                try:
                    response_obj_2.json() # Test if it's valid JSON
                    self.html2 = response_obj_2.text
                    print("[green]Successfully stored text content for html2.[/green]")
                except json.JSONDecodeError:
                    print(f"[bold red]Error: Response for url2 (Status {response_obj_2.status_code}) is not valid JSON despite content type.[/bold red]")
                    print(f"Response snippet: {response_obj_2.text[:200]}...") # Show beginning of bad response
            else:
                 print(f"[yellow]Warning: Unexpected content type for url2: '{content_type}'. Storing text anyway.[/yellow]")
                 self.html2 = response_obj_2.text # Store text even if content-type mismatch
                 # Alternatively: print("[bold red]Error: Expected JSON content type, but got '{content_type}'.[/bold red]")
        else:
            print("[bold red]Error: Request failed for url2 (non_curl_cffi_req returned None). Cannot proceed with this data.[/bold red]")
            # self.html2 remains None

        print("\n[blue]__init__ finished synchronous fetch attempts.[/blue]")


    def parse(self, response):
        """
        Processes the data fetched in __init__.
        This method gets called by Scrapy AFTER __init__ finishes.
        The 'response' argument here is for the start_url (httpbin.org),
        but we ignore it and use self.html1 / self.html2.
        """
        print("[blue]Parse method started. Using pre-fetched html1/html2.[/blue]")

        # --- CRUCIAL CHECK: Ensure data was actually fetched successfully in __init__ ---
        if self.html1 is None:
            print("[bold red]Error in parse: self.html1 is None. Fetch must have failed in __init__. Stopping.[/bold red]")
            # Optionally raise CloseSpider
            # raise CloseSpider("Failed to fetch data for backward_metric_1 in __init__")
            return # Stop processing

        if self.html2 is None:
            print("[bold red]Error in parse: self.html2 is None. Fetch must have failed in __init__. Stopping.[/bold red]")
            # Optionally raise CloseSpider or try to save partial data if desired
            # raise CloseSpider("Failed to fetch data for backward_metric_2 in __init__")
            return # Stop processing

        # --- Process Response 1 (self.html1) ---
        # Add try-except around json.loads as an extra layer of safety
        try:
            data1 = json.loads(self.html1) # Use self.html1
            print("[green]Successfully parsed JSON from self.html1[/green]")
        except json.JSONDecodeError as e:
            print(f"[bold red]Fatal Error in parse: Could not decode JSON from self.html1 even after __init__ checks: {e}[/bold red]")
            print(f"Data snippet (html1): {self.html1[:200]}...")
            raise CloseSpider(f"JSONDecodeError in parse for html1: {e}")

        # Check API's own status field if necessary (optional based on __init__ checks)
        if data1.get('head', {}).get('statusDescription') != "Success":
            print(f"[yellow]Warning in parse: API status for data1 is not 'Success': {data1.get('head', {}).get('statusDescription')}[/yellow]")
            # Decide if this is critical enough to stop
            # raise CloseSpider("API reported non-Success status for data1 in parse")

        # --- Header/Index Definitions and Data Extraction for Metric 1 (from original code) ---
        header_names_1 = [
            "Stock Name", "NSE Code", "BSE Code", "Stock Code", "ISIN",
            "Industry Name", "Current Price", "Market Capitalization", "Total Debt to Total Equity Annual",
            "Long Term Debt To Equity Annual", "Long Term Debt To Equity Annual 1Yr Ago",
            "Interest Coverage Ratio Annual", "Interest Coverage Ratio Annual 1Yr Ago",
            "ROE Annual %", "ROE Annual 1Yr Ago %", "ROE Annual 2Yr Ago %", "ROE Annual 3Yr Ago %",
            "ROE Annual 4Yr Ago %", "ROE Annual 5Yr Ago %", "ROCE Annual %", "ROCE Annual 1Yr Ago %",
            "RoA Annual %", "RoA Annual 1Yr Ago %", "RoA Annual 2Yr Ago %", "RoA Annual 3Yr Ago %",
            "RoA Annual 4Yr Ago %", "RoA Annual 5Yr Ago %", "Current Ratio Annual", "Quick Ratio Annual",
            "Working Capital Annual", "Total Assets Annual", "Total Assets Annual 1Yr Ago",
            "Total Assets Annual 2Yr Ago",
            "Total Assets Annual 3Yr Ago", "Total Assets Annual 4Yr Ago", "Total Assets Annual 5Yr Ago",
            "Total Current Assets Annual", "Total Current Assets Annual 1Yr Ago", "Total Non Current Assets Annual",
            "Book Value Per Share Annual", "Book Value Per Share Annual Adjusted",
            "Book Value Per Share Annual 1Yr Ago",
            "Book Value Per Share Annual 2Yr Ago", "Book Value per Share Annual 3Yr ago", "Book Value Per Share Latest",
            "Price to Sales TTM", "Total Revenue Annual", "Total Revenue Annual 1Yr Ago", "Revenue Annual 2Yr ago",
            "Revenue Annual 3Yr ago", "Revenue Annual 4Yr ago", "Revenue Annual 5Yr ago",
            "Forecaster Estimates 1Y ago Free Cash Flow Annual", "Forecaster Estimates 2Y ago Free Cash Flow Annual",
            "Forecaster Estimates year goneby Free Cash Flow Annual",
            "Forecaster Estimates 1Y forward Free Cash Flow Annual",
            "Forecaster Estimates 2Y forward Free Cash Flow Annual",
            "Forecaster estimates 1Y forward Free Cash Flow Growth Annual %",
            "dayHighLow", "weekHighLow", "monthHighLow", "qtrHighLow", "yearHighLow", "threeYearHighLow",
            "fiveYearHighLow", "tenYearHighLow", "Annualized Volatility"
        ]
        indexes_1 = [
            2, 3, 4, 5, 7,
            None, None, 14, 15, 16, 17, 18, 19,
            20, 21, 22, 23, 24, 25, 26, 27,
            28, 29, 30, 31, 32, 33, 34, 35, 36,
            37, 38, 39, 40, 41, 42, 43, 44, 45,
            46, 47, 48, 49, 50, 51, 52, 53, 54,
            55, 56, 57, 58, 59, 60, 61, 62, 63,
            64, 65, 66, 67, 68, 69, 70, 71, 72, 73
        ]

        # Store unstructured data
        if data1.get('body'):
            self.unstuctured_trendlyne_data["backward_metric_1"].append(data1['body'])

        # Extract structured data
        stock_data_1 = data1.get('body', {}).get('tableData', [])
        num_expected_fields_1 = max(filter(None, indexes_1)) + 1 if any(indexes_1) else 0
        for stock in stock_data_1:
             if isinstance(stock, list):
                 # Bounds check (simple version)
                 stock_len = len(stock)
                 # Extract with None for missing indexes or out-of-bounds access
                 stock_data_req = [
                     (stock[i] if i is not None and i < stock_len else None)
                     for i in indexes_1
                 ]
                 stock_data_with_headers = dict(zip(header_names_1, stock_data_req))
                 self.full_stock_data["backward_metric_1"].append(stock_data_with_headers)
             else:
                 print(f"[yellow]Warning: Item in tableData (metric 1) is not a list: {stock}[/yellow]")


        # --- Process Response 2 (self.html2) ---
        try:
            data2 = json.loads(self.html2) # Use self.html2
            print("[green]Successfully parsed JSON from self.html2[/green]")
        except json.JSONDecodeError as e:
            print(f"[bold red]Fatal Error in parse: Could not decode JSON from self.html2: {e}[/bold red]")
            print(f"Data snippet (html2): {self.html2[:200]}...")
            # If html1 processing worked, maybe save that partial data before closing?
            # self.save_partial_data() # Example function call
            raise CloseSpider(f"JSONDecodeError in parse for html2: {e}")

        # Check API's own status field if necessary
        if data2.get('head', {}).get('statusDescription') != "Success":
            print(f"[yellow]Warning in parse: API status for data2 is not 'Success': {data2.get('head', {}).get('statusDescription')}[/yellow]")
            # Decide if this is critical
            # raise CloseSpider("API reported non-Success status for data2 in parse")

        # --- Header/Index Definitions and Data Extraction for Metric 2 (from original code) ---
        header_names_2 = [
            "Stock Name", "NSE Code", "BSE Code", "Stock Code", "ISIN", "Industry Name", "Current Price",
            "Market Capitalization", "Insider Pledge Change as %Total Shares Today",
            "Insider Pledge Change as %Total Shares Yesterday", "Insider Pledge Change as %Total Shares Last Week",
            "Insider Pledge Change as %Total Shares Last Month", "Insider Pledge invoked as %Total Shares Today",
            "Insider Pledge invoked as %Total Shares Yesterday", "Insider Pledge invoked as %Total Shares Last Week",
            "Insider Pledge invoked as %Total Shares Last Month", "Insider Buys as %Total Shares Today",
            "Insider Buys as %Total Shares Yesterday", "Insider Buys as %Total Shares Last Week",
            "Insider Buys as %Total Shares Last Month", "Insider Buys as %Total Shares Last Quarter",
            "Insider Sells as %Total Shares Today", "Insider Sells as %Total Shares Yesterday",
            "Insider Sells as %Total Shares Last Week", "Insider Sells as %Total Shares Last Month",
            "Insider Sells as %Total Shares Last Quarter", "DVM_classification_text", "NSE code", "BSE code",
            "ISIN", "Dividend Rate Annual %", "Dividend in past 12M", "Dividend 1Yr adjusted",
            "Dividend Sum 2Yr adjusted", "Dividend Sum 3Yr adjusted", "Dividend Sum 5Yr adjusted",
            "Dividend 1year", "Dividend Sum 2Yr", "Dividend Sum 3Yr", "Dividend Sum 5Yr",
            "Dividend payout ratio preceding TTM %", "Dividend payout ratio TTM %", "Dividend yield 1yr %",
            "Dividend yield 2yr %", "Dividend yield 3yr %", "Dividend yield 5yr %",
            "Dividend payout ratio 2Yr %", "Beta 1Month", "Beta 1Year", "Beta 3Month", "Beta 3Year",
            "Forecaster Estimates 3Mth Analysts Upgrade",
            "Forecaster Estimates No of analysts with strong buy reco", "Forecaster Estimates No of analysts with hold reco",
            "Forecaster Estimates No of analysts with buy reco", "Forecaster Estimates No of analysts with strong sell reco",
            "Forecaster Estimates No of analysts with sell reco", "EnterpriseValue Annual", "EV Per Net Sales Annual",
            "Industry PE TTM", "Industry PEG TTM", "PEG TTM PE to Growth", "Sector PE TTM", "Sector PEG TTM",
            "Forecaster Estimates 1Q ago EBIT Quarter", "Forecaster Estimates 2Q ago EBIT Quarter",
            "Forecaster Estimates quarter goneby EBIT Quarter"
        ]
        indexes_2 = [
            2, 3, 4, None, 7, None, None, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
            31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
            55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73
        ]

        # Store unstructured data
        if data2.get('body'):
             self.unstuctured_trendlyne_data["backward_metric_2"].append(data2['body'])

        # Extract structured data
        stock_data_2 = data2.get('body', {}).get('tableData', [])
        num_expected_fields_2 = max(filter(None, indexes_2)) + 1 if any(indexes_2) else 0
        for stock in stock_data_2:
            if isinstance(stock, list):
                stock_len = len(stock)
                stock_data_req = [
                    (stock[i] if i is not None and i < stock_len else None)
                    for i in indexes_2
                ]
                stock_data_with_headers = dict(zip(header_names_2, stock_data_req))
                self.full_stock_data["backward_metric_2"].append(stock_data_with_headers)
            else:
                 print(f"[yellow]Warning: Item in tableData (metric 2) is not a list: {stock}[/yellow]")


        # --- Save Data (from original code) ---
        print("[blue]Saving combined data to files...[/blue]")
        current_date_str = today().strftime("%Y-%m-%d")

        # Save structured data
        json_data_structured = {
            "date": current_date_str,
            "backward_metric_1_data": self.full_stock_data["backward_metric_1"],
            "backward_metric_2_data": self.full_stock_data["backward_metric_2"]
        }
        try:
            json_line = json.dumps(json_data_structured)
            with open(self.jsonl_file_path_structured, "a", encoding="utf-8") as file:
                file.write(json_line + "\n")
            print(f"[green]Saved structured data to {self.jsonl_file_path_structured}[/green]")
        except IOError as e:
             print(f"[bold red]Error writing structured file: {e}[/bold red]")
        except TypeError as e:
             print(f"[bold red]Error serializing structured data: {e}[/bold red]")


        # Save unstructured data
        json_data_unstructured = {
            "date": current_date_str,
            "backward_metric_1_data": self.unstuctured_trendlyne_data["backward_metric_1"],
            "backward_metric_2_data": self.unstuctured_trendlyne_data["backward_metric_2"]
        }
        try:
            json_line = json.dumps(json_data_unstructured)
            with open(self.jsonl_file_path_unstuctured_trendlyne_data, "a", encoding="utf-8") as file:
                file.write(json_line + "\n")
            print(f"[green]Saved unstructured data to {self.jsonl_file_path_unstuctured_trendlyne_data}[/green]")
        except IOError as e:
            print(f"[bold red]Error writing unstructured file: {e}[/bold red]")
        except TypeError as e:
            print(f"[bold red]Error serializing unstructured data: {e}[/bold red]")

        print("[blue]Parse method finished.[/blue]")

# --- Make sure you have the necessary imports at the top of your file ---
# import scrapy
# import requests
# import json
# from datetime import datetime
# from dateutil.utils import today
# from scrapy.exceptions import CloseSpider
# from rich import print