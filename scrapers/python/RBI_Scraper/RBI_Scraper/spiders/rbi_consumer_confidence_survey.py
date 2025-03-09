# Need to schedule this on 10th of every even month
import json
import os
from datetime import datetime
from rich import print
import pandas as pd
import scrapy
from scrapy.exceptions import CloseSpider


# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want



class RbiConsumerConfidenceSurveySpider(scrapy.Spider):
    name = "rbi_consumer_confidence_survey"
    allowed_domains = ["website.rbi.org.in"]
    start_urls = ["https://website.rbi.org.in/web/rbi/statistics/survey?category=24927731&categoryName=Consumer%20Confidence%20Survey%20-%20Bi-monthly&delta=100"]
    CSV_FILE = "rbi_consumer_confidence_survey_last_date.csv"
    # Define file path
    json_file_path = r"D:\Desktop\financial_data_pipeline\data\raw\RBI_data\consumer_conf_survey.json"

    def parse(self, response):
        raw_date = response.xpath('//div[@class="notification-date font-resized"]/text()').get().strip()
        extracted_date = datetime.strptime(raw_date, "%b %d, %Y").date()

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

        yield scrapy.Request(url=latest_url, callback=self.parse_consumer_confidence_survey, cb_kwargs=dict(date=extracted_date))

    def parse_consumer_confidence_survey(self, response, date):
        tables = response.xpath('//table[@class="tablebg"]')
        table_titles = [
            "T1 Economic Situation", "T2 Employment", "T3 Prices", "T4 Inflation",
            "T5 Income", "T6 Spending", "T7 Essential spending", "T8 Non-essential spending"
        ]

        results = {}

        for idx, table in enumerate(tables):
            if idx == 3:  # Table 4 (Inflation) needs 2nd last row
                last_row = table.xpath('.//tr[position()=last()-1]/td/text()').getall()
            else:
                last_row = table.xpath('.//tr[last()]/td/text()').getall()  # Last row for others

            if len(last_row) == 9:  # Ensure correct number of columns
                results[table_titles[idx]] = {
                    "Current Perception - Improved": float(last_row[1]),
                    "Current Perception - Remained Same": float(last_row[2]),
                    "Current Perception - Worsened": float(last_row[3]),
                    "Current Perception - Net Response": float(last_row[4]),
                    "One Year Ahead Expectation - Will Improve": float(last_row[5]),
                    "One Year Ahead Expectation - Will Remain Same": float(last_row[6]),
                    "One Year Ahead Expectation - Will Worsen": float(last_row[7]),
                    "One Year Ahead Expectation - Net Response": float(last_row[8])
                }
            survey_date = datetime.strptime(last_row[0], "%b-%y")

        formatted_date = survey_date.date().strftime("%Y-%m-%d 00:00:00")  # Format for JSON

        # Initialize sums
        total_current_net_response = 0
        total_future_net_response = 0
        total_tables = len(results)

        # Sum up Net Responses
        for table_data in results.values():
            total_current_net_response += table_data["Current Perception - Net Response"]
            total_future_net_response += table_data["One Year Ahead Expectation - Net Response"]

        # Compute CSI and FEI
        csi = 100 + (total_current_net_response / total_tables)
        fei = 100 + (total_future_net_response / total_tables)

        # Add Table 9 results
        results["T9 Indices"] = {
            "CSI": round(csi, 2),
            "FEI": round(fei, 2)
        }


        # Load existing data if file exists, else create an empty dictionary
        if os.path.exists(self.json_file_path) and os.path.getsize(self.json_file_path) > 0:
            with open(self.json_file_path, "r", encoding="utf-8") as file:
                data = json.load(file)  # Load existing JSON data
        else:
            data = {}

        # Append new data
        data[formatted_date] = results

        # Write back to the JSON file
        with open(self.json_file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)  # Pretty-print JSON

        print(f"Data appended successfully to {self.json_file_path}")

        self.log(print(data[formatted_date]))