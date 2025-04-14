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
from RBI_Scraper.helper_func import safe_float


class OldSiteRbiIipSpider(scrapy.Spider):
    name = "old_site_rbi_iip"
    allowed_domains = ["rbi.org.in"]
    start_urls = ["https://rbi.org.in/Scripts/BS_ViewBulletin.aspx"]
    year_of_data = 0
    CSV_FILE = "rbi_iip_last_date.csv"
    # Define file path
    json_file_path = r"D:\Desktop\financial_data_pipeline\data\raw\RBI_data\Index_of_Industrial_Production_(IIP-2011-12=100).json"

    def parse(self, response):
        date_text = response.xpath(
            '//table[@class="tablebg"]//td[@class="tableheader"]/b/following-sibling::text()[1]').get().strip()
        if date_text:
            # Clean up the text by stripping extra whitespace
            date_text = date_text.strip()
            self.logger.info("Extracted Date: %s", date_text)
            extracted_date = datetime.strptime(date_text, "%b %d, %Y").date()

        # if os.path.exists(self.CSV_FILE):
        #     df = pd.read_csv(self.CSV_FILE)
        #     last_saved_date = datetime.strptime(df.iloc[0, 0], "%Y-%m-%d").date()  # Read and convert to date
        #
        #     if extracted_date == last_saved_date:
        #         self.log(extracted_date)
        #         raise CloseSpider("The new Consumer Survey is yet to be uploaded by the RBI.")

        # If new date, update CSV and proceed with scraping
        df = pd.DataFrame([[extracted_date]], columns=["date"])
        df.to_csv(self.CSV_FILE, index=False)

        relative_url = response.xpath('//a[contains(text(), " Index of Industrial Production")]/@href').get()
        absolute_url = f'https://rbi.org.in/Scripts/{relative_url}'
        self.log(absolute_url)
        yield scrapy.Request(url=absolute_url, callback=self.parse_iip, cb_kwargs=dict(date=extracted_date))

    def parse_iip(self, response, date):
        year_extracted = date.year
        month_extracted = date.month

        # If the month is January (1) or February (2), subtract 1 from the year.
        self.year_of_data = year_extracted - 1 if month_extracted in [1, 2] else year_extracted

        print("year_of_data:", self.year_of_data)

        outer_table = response.xpath('//table[@class="tablebg"]')
        table = outer_table.xpath(".//table")

        categories_iip = [
            "General Index",
            "Sectoral Classification-Mining",
            "Sectoral Classification-Manufacturing",
            "Sectoral Classification-Electricity",
            "Use-Based Classification-Primary Goods",
            "Use-Based Classification-Capital Goods",
            "Use-Based Classification-Intermediate Goods",
            "Use-Based Classification-Infrastructure/Construction Goods",
            "Use-Based Classification-Consumer Durables",
            "Use-Based Classification-Consumer Non-Durables"
        ]

        # Create a mapping: cleaned_category -> original_category
        category_mapping = {}
        for category in categories_iip:
            # Remove left-side numbering with a regex:
            try:
                if re.search(r'^Use', category,
                              re.IGNORECASE):
                    cleaned = category.split("-")[2]
                else:
                    cleaned = category.split("-")[1]
            except:
                cleaned = category.split("-")[0]

            # cleaned = re.sub(r'^[A-Z0-9.]+\)\s*', '', category)

            category_mapping[cleaned] = category

        print(category_mapping)
        results = {}

        categories = categories_iip

        # finding date for data
        rows = table.xpath('.//tr[td]')

        condition_met = False

        for row in rows:

            # Get all td elements in the row
            tds = row.xpath('./td')

            row_text = ''.join(tds.xpath('.//text()').getall()).strip()

            # Apply condition only if it hasn't been met yet
            if not condition_met and (
                    "Survey period ended" in row_text or
                    re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*', row_text,
                              re.IGNORECASE)
            ):
                condition_met = True  # Mark condition as met
                date_selector = tds[-1]
                extracted_date_final = self.convert_date_month_year_to_be_given_seperately(date_selector.xpath('string()').get().strip(),
                                                                                           year_of_data =  self.year_of_data,
                                                                                           var_name= "extracted_date_final")
                break

        print(condition_met)

        # Extracting data

        rows = table.xpath('.//tr[number(string(td[last()])) = number(string(td[last()]))]')
        for i, row in enumerate(rows):

            data = [text.strip() for text in row.xpath('./td//text()').getall() if text.strip()]
            # Initialize table_name
            table_name = None

            element = data[0]
            print(element)

            # Check for partial matches using regex (case-insensitive)
            for cleaned_category, original_category in category_mapping.items():
                if re.search(rf'{re.escape(cleaned_category)}', element, re.IGNORECASE):
                    table_name = original_category
                    print(f"Match found: {table_name}")
                    break

            # handling edge cases

            if re.search(r'Infrastructure', element, re.IGNORECASE):
                table_name = "Use-Based Classification-Infrastructure/Construction Goods"

            # if re.search(r'Prepared meals, snacks', element, re.IGNORECASE):
            #     table_name = "A.1.12) Prepared meals, snacks, sweets etc."

            # print(data)

            if table_name is not None:

                td_count = len(data)

                # Check if row has more than 6 <td> elements
                if td_count > 7:
                    results[table_name] = safe_float(data[-1])

        scraped_data = {
            extracted_date_final : results,
        }

        print(scraped_data)

        ##### for JSON file path

        # Load the existing JSON data from file (if exists)
        if os.path.exists(self.json_file_path):
            with open(self.json_file_path, "r") as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = {}
        else:
            existing_data = {}


            # Process each date in the scraped data
        for date, data_types in scraped_data.items():
            # If the date doesn't exist in the JSON file, add it entirely.
            if date not in existing_data:
                existing_data[date] = data_types
            else:
                self.log("data already exists")
                raise CloseSpider(
                    f'Data with date {extracted_date_final} is already present with full data according to info above. Closing spider.')

        # Write back to the JSON file
        with open(self.json_file_path, "w") as f:
            json.dump(existing_data, f, indent=4)

        print(f"Data appended successfully to {self.json_file_path}")

        ##### for JSONl file path


        # json_data = {
        #     extracted_date_final :{"Final": results_data_final},
        #     extracted_date_provisional:{"Provisional": results_data_provisional}
        # }
        # print(json_data)
        # json_line = json.dumps(json_data)
        #
        #
        # with open(self.jsonl_file_path, "a", encoding="utf-8") as file:
        #     file.write(json_line + "\n")
        #
        # print(f"Data appended to {self.jsonl_file_path}")

    def convert_date_month_year_to_be_given_seperately(self, month_day_of_data,year_of_data,var_name=None):
        try:
            # Remove any periods from the abbreviated month, e.g. "Dec. 27" -> "Dec 27"
            month_day_clean = month_day_of_data.replace('.', ' ').replace(' (P)', '')

            # Combine into a date string like " Dec 2025"
            date_str = f"{month_day_clean} {year_of_data}"

            # Parse the string into a datetime object. The format %Y for the year, %b for the abbreviated month, and %d for the day.
            constructed_date = datetime.strptime(date_str, "%B %Y")
            formatted_date = constructed_date.date().strftime("%Y-%m-%d")  # Format for JSON
            return formatted_date

        except Exception:
            error_message = traceback.format_exc()
            if var_name:
                print(error_message)
                raise CloseSpider(f"An error occurred while assigning to variable '{var_name}':")
            print(error_message)
            return None
