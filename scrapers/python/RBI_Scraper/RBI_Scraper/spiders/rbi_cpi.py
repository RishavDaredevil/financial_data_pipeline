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


class RbiCpiSpider(scrapy.Spider):
    name = "rbi_cpi"
    allowed_domains = ["website.rbi.org.in"]
    start_urls = ["https://website.rbi.org.in/web/rbi/publications/rbi-bulletin"]
    year_of_data = 0
    CSV_FILE = "rbi_cpi_last_date.csv"
    # Define file path
    json_file_path = r"D:\Desktop\financial_data_pipeline\data\raw\RBI_data\CPI-Rural_Urban_Combined.json"

    def parse(self, response):
        raw_date = response.xpath('(//span[@class="mtm_list_item_heading truncatedContent font-resized"])[1]/text()').get().strip()
        date_part = raw_date.split(" - ")[-1]
        extracted_date = datetime.strptime(date_part, "%b %d, %Y").date()

        if os.path.exists(self.CSV_FILE):
            df = pd.read_csv(self.CSV_FILE)
            last_saved_date = datetime.strptime(df.iloc[0, 0], "%Y-%m-%d").date()  # Read and convert to date

            if extracted_date == last_saved_date:
                self.log(extracted_date)
                raise CloseSpider("The new Consumer Survey is yet to be uploaded by the RBI.")

        # If new date, update CSV and proceed with scraping
        df = pd.DataFrame([[extracted_date]], columns=["date"])
        df.to_csv(self.CSV_FILE, index=False)

        latest_url = response.xpath('(//a[@class="mtm_list_item_heading"])[1]/@href').get()
        absolute_url = f'https://website.rbi.org.in{latest_url}'
        self.log(absolute_url)
        yield scrapy.Request(url=absolute_url, callback=self.parse_bulletin, cb_kwargs=dict(date=extracted_date))

    def parse_bulletin(self, response, date):

        relative_url = re.findall(r'href="([^"]+consumer-price-index[^"]*)"', response.text)[0]
        absolute_url = f'https://website.rbi.org.in{relative_url}'
        yield scrapy.Request(url=absolute_url, callback=self.parse_cpi, cb_kwargs=dict(date=date))

    def parse_cpi(self, response, date):
        # year_extracted = date.year
        # month_extracted = date.month
        #
        # # If the month is January (1) or February (2), subtract 1 from the year.
        # self.year_of_data = year_extracted - 1 if month_extracted in [1, 2] else year_extracted
        #
        # print("year_of_data:", self.year_of_data)

        table = response.xpath('//div[@class="tablebg"]/table')

        categories_cpi = [
            "A) General Index",
            "A.1) Food and beverages",
            "A.1.1) Cereals and products",
            "A.1.10) Spices",
            "A.1.11) Non-alcoholic beverages",
            "A.1.12) Prepared meals, snacks, sweets etc.",
            "A.1.2) Meat and fish",
            "A.1.3) Egg",
            "A.1.4) Milk and products",
            "A.1.5) Oils and fats",
            "A.1.6) Fruits",
            "A.1.7) Vegetables",
            "A.1.8) Pulses and products",
            "A.1.9) Sugar and confectionery",
            "A.2) Pan, tobacco and intoxicants",
            "A.3) Clothing and footwear",
            "A.3.1) Clothing",
            "A.3.2) Footwear",
            "A.4) Housing",
            "A.5) Fuel and light",
            "A.6) Miscellaneous",
            "A.6.1) Household goods and services",
            "A.6.2) Health",
            "A.6.3) Transport and communication",
            "A.6.4) Recreation and amusement",
            "A.6.5) Education",
            "A.6.6) Personal Care and Effects",
            "B) Consumer Food Price Index"
        ]

        # Create a mapping: cleaned_category -> original_category
        category_mapping = {}
        for category in categories_cpi:
            # Remove left-side numbering with a regex:
            cleaned = re.sub(r'^[A-Z0-9.]+\)\s*', '', category)
            category_mapping[cleaned] = category

        results_data_final = {}

        results_data_provisional = {}

        categories = categories_cpi

        # finding date for data
        rows = table.xpath('.//tbody/tr[td]')

        condition_met = False

        for row in rows:

            # Get all td elements in the row
            tds = row.xpath('./td')

            row_text = ''.join(tds.xpath('.//text()').getall()).strip()

            # Apply condition only if it hasn't been met yet
            if not condition_met and (
                    "Survey period ended" in row_text or
                    re.search(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.\d{2}', row_text,
                              re.IGNORECASE)
            ):
                condition_met = True  # Mark condition as met
                date_selector = tds[-2:]
                extracted_date_final = self.convert_date_numeric_is_year(date_selector[0].xpath('string()').get().strip(),var_name= "extracted_date_final")
                extracted_date_provisional = self.convert_date_numeric_is_year(date_selector[1].xpath('string()').get().strip(),var_name= "extracted_date_provisional")
                break


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

            if re.search(r'Meat and', element, re.IGNORECASE):
                table_name = "A.1.2) Meat and fish"

            if re.search(r'Prepared meals, snacks', element, re.IGNORECASE):
                table_name = "A.1.12) Prepared meals, snacks, sweets etc."

            # print(data)

            # if table_name is None:
            #     table_name = "Part-time/Contractual/Outsourced Employees"

            if table_name is not None:

                td_count = len(data)

                # Check if row has more than 6 <td> elements
                if td_count > 12:
                    # Process final results
                    rural_index_final = safe_float(data[5])
                    urban_index_final = safe_float(data[8])
                    combined_index_final = safe_float(data[-2])

                    results_data_final[table_name] = {
                        "Rural-Index": rural_index_final,
                        "Urban-Index": urban_index_final,
                        "Combined-Index": combined_index_final,
                    }

                    # Process provisional results
                    rural_index_prov = safe_float(data[6])
                    rural_base = safe_float(data[4])
                    if rural_index_prov is None or rural_base is None or rural_base == 0:
                        rural_inflation = None
                    else:
                        rural_inflation = (rural_index_prov - rural_base) / rural_base

                    urban_index_prov = safe_float(data[9])
                    urban_base = safe_float(data[7])
                    if urban_index_prov is None or urban_base is None or urban_base == 0:
                        urban_inflation = None
                    else:
                        urban_inflation = (urban_index_prov - urban_base) / urban_base

                    combined_index_prov = safe_float(data[-1])
                    combined_base = safe_float(data[10])
                    if combined_index_prov is None or combined_base is None or combined_base == 0:
                        combined_inflation = None
                    else:
                        combined_inflation = (combined_index_prov - combined_base) / combined_base

                    results_data_provisional[table_name] = {
                        "Rural-Index": rural_index_prov,
                        "Rural-Inflation": rural_inflation,
                        "Urban-Index": urban_index_prov,
                        "Urban-Inflation": urban_inflation,
                        "Combined-Index": combined_index_prov,
                        "Combined-Inflation": combined_inflation
                    }

        ##### for JSON file path

        scraped_data = {
            extracted_date_final :{"Final": results_data_final},
            extracted_date_provisional:{"Provisional": results_data_provisional}
        }

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
                # The date exists – check each data type (Final/Provisional)
                for type_key, new_content in data_types.items():
                    # If the type is not in the JSON data for the date, add it.
                    if type_key not in existing_data[date]:
                        existing_data[date][type_key] = new_content
                    else:
                        print("date with the same type(Provisional/Final) already exists")

            # Write the updated data back to the file
        with open(self.json_file_path, "w") as f:
            json.dump(existing_data, f, indent=4)

        print(f"Data appended to {self.json_file_path}")

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

    def convert_date_numeric_is_year(self, month_day_of_data,var_name=None):
        try:
            # Remove any periods from the abbreviated month, e.g. "Dec. 27" -> "Dec 27"
            month_day_clean = month_day_of_data.replace('.', ' ').replace(' (P)', '')

            # Combine into a date string like "2025 Dec 27"
            date_str = f"{month_day_clean}"

            # Parse the string into a datetime object. The format %Y for the year, %b for the abbreviated month, and %d for the day.
            constructed_date = datetime.strptime(date_str, "%b %y")
            formatted_date = constructed_date.date().strftime("%Y-%m-%d")  # Format for JSON
            return formatted_date

        except Exception:
            error_message = traceback.format_exc()
            if var_name:
                raise CloseSpider(f"An error occurred while assigning to variable '{var_name}':")
            else:
                print("An error occurred:")
            print(error_message)
            return None
