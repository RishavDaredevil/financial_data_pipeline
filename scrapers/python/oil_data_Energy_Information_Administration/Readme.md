# EIA Crude Oil Spot Price Scraper Documentation

## 1. Overview

This Scrapy spider (`eia_crude_spot.py`) is designed to scrape daily spot price data for WTI (Cushing) and Brent crude oil from the U.S. Energy Information Administration (EIA) API v2.

Key features include:
* Fetching data only for specified crude oil series (WTI & Brent).
* Using a control CSV file (`eia_crude_spot_last_date.csv`) to store the latest date of data retrieved in the previous run.
* Implementing logic to **prevent execution** if it determines that no new data is likely available based on the date in the control CSV and an assumed weekly update schedule.
* Appending new data in JSON Lines format to separate files for WTI and Brent.
* Tracking the latest date encountered within the scraped data during the run.
* Updating the control CSV file with the latest date found *after* successfully processing all data pages in the current run.

## 2. EIA API v2 Usage

The spider interacts with the EIA API v2 endpoint for petroleum spot prices.

* **Endpoint:** `https://api.eia.gov/v2/petroleum/pri/spt/data`
* **Key Parameters Used:**
    * `api_key`: (Required by EIA) Your unique API key for authentication. Stored in the spider configuration.
    * `frequency=daily`: Specifies that only daily data points should be retrieved.
    * `data[]=value`: Requests the actual spot price data point (column named `value`).
    * `facets[series][]=RWTC` & `facets[series][]=RBRTE`: Filters the data to include only the series corresponding to WTI Cushing (`RWTC`) and Europe Brent (`RBRTE`).
    * `length=5000`: Requests the maximum number of rows per API response page allowed by the EIA API (for JSON) to minimize the number of requests.
    * `offset=<number>`: Used internally by the spider for pagination. It tells the API how many records to skip, allowing the spider to fetch subsequent pages of data. Starts at 0 and increases by the number of records received per page.
    * `start=<YYYY-MM-DD>`: Used **only for the initial request** if the control CSV indicates that scraping should proceed. It's set to the day *after* the date stored in the control CSV, ensuring only data newer than the last run is requested initially. This parameter is **not** used in subsequent pagination requests.

## 3. Control CSV File (`eia_crude_spot_last_date.csv`)

This CSV file acts as a control mechanism to manage spider execution based on the assumed data update schedule.

* **Purpose:** Stores the date of the **latest data point (`period`)** that was successfully scraped and saved during the *previous* run. It prevents the spider from running unnecessarily if no new data is expected.
* **Location:** Must be in the same directory as the spider script.
* **Format:** A simple CSV file with a single column header named `date` and one row below it containing the latest date in `YYYY-MM-DD` format.
    ```csv
    date
    2025-04-07
    ```
* **Initial Creation:** If the file doesn't exist when the spider first runs, the spider will proceed without a `start` date filter (fetching all available historical data) and will create the file upon completion with the latest date found.

## 4. Execution Control Logic (`_read_start_date`)

The spider implements logic to check if running it is necessary based on an assumed weekly data release schedule. The EIA data is often updated weekly, and the latest available data might pertain to a date a couple of days prior to the release day.

1.  **Calculate Expected Last Data Date:** The script calculates `last_date_spider_run` by subtracting 9 days from the current date (`today - timedelta(days=9)`). **Assumption:** This calculation approximates the date of the *most recent data that should have been available* if the spider is run just *before* the next expected weekly update. *You may need to adjust the `timedelta(days=9)` based on observing the actual EIA release timing for this specific dataset.*
2.  **Read Last Saved Date:** It reads the date from the control CSV file (`eia_crude_spot_last_date.csv`).
3.  **Compare Dates:** It compares the calculated `last_date_spider_run` with the `last_saved_date` from the CSV file using `<=`.
4.  **Decision:**
    * **If `last_date_spider_run <= last_saved_date`:** This means the date saved in the CSV is the same or *newer* than the calculated expected date of the last available data. The script assumes no new data has been released yet. It logs a message and immediately **stops the entire Python script** using `sys.exit("...")`. **Note:** This abrupt stop prevents further spider execution *and* prevents the Scrapy `spider_closed` signals/pipelines from running for *this specific exit scenario*.
    * **If `last_date_spider_run > last_saved_date`:** This means the date in the CSV is older than expected, suggesting new weekly data *might* be available. The script calculates the `start` date for the API request as the day *after* the `last_saved_date` and returns it. The spider then proceeds with the scraping process.
5.  **Error Handling:** If the CSV is missing, empty, or contains invalid data, the script logs a warning/error and returns `None`, causing the spider to run without a `start` date filter (effectively fetching all data since the beginning or potentially re-fetching data).

## 5. Data Processing and Output

* **Fetching:** The spider sends an initial request (potentially with a `start` date) and then uses the `offset` parameter in subsequent requests (`parse` method) to fetch all data matching the criteria, page by page.
* **Tracking Latest Date:** Within the `parse` method, as each data record (`item`) is processed, its `period` date is extracted and compared to the `self.latest_date_scraped` attribute. This attribute is updated whenever a newer date is encountered, ensuring it holds the latest date found across *all* data scraped in the *current* run.
* **Saving Data:** Items yielded by the spider are passed to the `MultiSortedJsonLinesPipeline` (defined in `pipelines.py`, referenced via `custom_settings`).
    * The pipeline buffers items separately for `RWTC` and `RBRTE`.
    * When the spider closes, the pipeline sorts the buffered items for each series by the `period` date.
    * It then **appends** the sorted items to the respective output files in **JSON Lines (.jsonl)** format (one JSON object per line).
    * **Output Files:**
        * `D:\Desktop\financial_data_pipeline\data\raw\Oil data\oil_data_Energy_Information_Administration\wti_crude_spot_prices.jsonl`
        * `D:\Desktop\financial_data_pipeline\data\raw\Oil data\oil_data_Energy_Information_Administration\brent_crude_spot_prices.jsonl`

## 6. Updating the Control CSV (`eia_crude_spot_last_date.csv`)

Unlike previous iterations that might have used `spider_closed`, **this version updates the control CSV within the `parse` method** after processing the *last page* of results.

1.  **Trigger:** The update occurs when the pagination logic determines there are no more pages to fetch (`else:` block after the pagination `if next_offset < total_records_int...`).
2.  **Action:**
    * It checks if `self.latest_date_scraped` (which holds the latest date found in the *current* run's data) has a value.
    * If it does, it creates a pandas DataFrame containing only this latest date.
    * It then **overwrites** the `eia_crude_spot_last_date.csv` file with this new DataFrame, ensuring the file contains the header `date` and the single latest date found in the data just scraped.
3.  **Purpose:** This prepares the CSV file for the *next* execution of the spider.

## 7. Running the Spider

1.  **Prerequisites:**
    * Ensure Scrapy and Pandas are installed (`pip install scrapy pandas`).
    * Make sure the control CSV file (`eia_crude_spot_last_date.csv`) exists in the same directory as the script, containing a valid date under the `date` header (or allow the script to create it on the first full run).
    * Make sure the pipeline definition (`MultiSortedJsonLinesPipeline`) is correctly referenced in `custom_settings` (e.g., located in `pipelines.py` within a Scrapy project structure).
2.  **Command:** Navigate to the directory containing the script in your terminal and run:
    ```bash
    scrapy runspider eia_crude_spot.py
    ```

## 8. Dependencies

* Scrapy
* Pandas