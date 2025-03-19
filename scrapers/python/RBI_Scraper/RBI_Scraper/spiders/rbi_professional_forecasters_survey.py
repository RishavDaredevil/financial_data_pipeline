import json
import os
import re
from datetime import datetime
from rich import print
import pandas as pd
import scrapy
from scrapy.exceptions import CloseSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class RbiProfessionalForecastersSurveySpider(CrawlSpider):
    name = "rbi_professional_forecasters_survey"
    allowed_domains = ["website.rbi.org.in"]
    start_urls = ["https://website.rbi.org.in/web/rbi/publications/articles?category=24927066&delta=100"]
    jsonl_file_path = r"D:\Desktop\financial_data_pipeline\data\raw\RBI_data\professional_forecasters_survey.jsonl"

    rules = (Rule(LinkExtractor(restrict_xpaths="//span[@class='mtm_list_item_heading truncatedContent']/ancestor::a"),
                  callback="parse_professional_forecasters_survey", follow=True),)

    def parse_professional_forecasters_survey(self, response):
        page_table_this_has_other_tables = response.xpath('//*[@id="elementIdCopy"]/table')
        table_1 = page_table_this_has_other_tables.xpath(".//table[1]")

        table_2 = page_table_this_has_other_tables.xpath(".//table[tbody[1]/tr[1]/td[contains(string(), 'Annual Forecasts')]]")

        table_3 = page_table_this_has_other_tables.xpath(".//table[tbody[1]/tr[1]/td[contains(string(), 'Quarterly Forecasts')]]")

        table_4 = page_table_this_has_other_tables.xpath(".//table[tbody[1]/tr[1]/td[contains(string(), 'Forecasts of CPI')]]")

        categories_table_1 = ["Real GDP", "Agriculture and Allied Activities", "Industry", "Services"]

        categories_table_2 = [
            "GDP at ",
            "Agriculture & Allied Activities",
            "Industry",
            "Services",
            "Fiscal Deficit of Central Govt.",
            "Combined Gross Fiscal Deficit",
            "Yield on 10-Year G-Sec",
            "Yield on 91-day",
            "Inflation based on CPI Combined: Headline",
            "Inflation based on CPI Combined: excluding Food and Beverages, Pan, Tobacco and Intoxicants and Fuel and Light",
            "Inflation based on WPI: All Commodities",
            "Inflation based on WPI: Non-food Manufactured Products"
        ]

        categories_table_3 = [
            "GDP at",
            "Agriculture & Allied Activities",
            "Industry",
            "Services",
            "IIP(Index of Industrial Production) (2011-12=100)",
            "Rupee per US $ Exchange rate",
            "Crude Oil",
            "Repo Rate"
        ]

        relative_url = re.findall(r'> Published on (.*) <', response.text)

        print(relative_url)


    #     results = {}
    #
    #     for table_no, table in enumerate(tables):
    #         if table_no == 0 or table_no == 2:
    #             Category = "Services"
    #             categories = categories_services
    #         else:
    #             Category = "Infrastructure"
    #             categories = categories_infrastructure
    #
    #         if table_no == 0 or table_no == 1:
    #
    #             # finding date for data
    #             rows = table.xpath('.//tbody/tr[td]')
    #
    #             condition_met = False
    #
    #             for row in rows:
    #
    #                 # Get all td elements in the row
    #                 tds = row.xpath('./td')
    #
    #                 row_text = ''.join(row.xpath('.//text()').getall()).strip()
    #                 # Apply condition only if it hasn't been met yet
    #                 if not condition_met and (
    #                         re.search(r'Q\d.+', row_text,
    #                                   re.IGNORECASE)
    #                 ):
    #                     condition_met = True  # Mark condition as met
    #                     date_selector = tds[1]
    #                     extracted_date = self.convert_quarter_date(date_selector.xpath('string()').get().strip())
    #                     break
    #
    #
    #             if Category not in results:
    #                 results[Category] = {}
    #
    #             # Extracting data
    #
    #             rows = table.xpath('.//tr[number(string(td[last()])) = number(string(td[last()]))]')
    #             for i, row in enumerate(rows):
    #
    #                 data = row.xpath('./td//text()').getall()
    #                 # Initialize table_name
    #                 table_name = None
    #
    #                 element = data[0]
    #
    #                 # Check for partial matches using regex (case-insensitive)
    #                 for category in categories:
    #                     if re.search(rf'\b{re.escape(element)}\b', category, re.IGNORECASE):
    #                         table_name = category
    #                         print(f"Match found: {table_name}")
    #                         break
    #
    #                 # handling edge cases
    #
    #                 if re.search(r'^Salary', element, re.IGNORECASE):
    #                     table_name = "Salary/Wages (D-I)"
    #
    #                 if table_name is None:
    #                     table_name = "Part-time/Contractual/Outsourced Employees"
    #
    #                 if table_name not in results[Category]:
    #                     results[Category][table_name] = {}
    #
    #                 td_count = len(data)
    #
    #                 # Check if row has more than 6 <td> elements
    #                 if td_count > 4:
    #                     results[Category][table_name] = {
    #                         "Assessment - Net Res": float(data[2]),
    #                         "Expectation for 1 qtr ahead - Net Res": float(data[4])
    #                     }
    #
    #         elif table_no == 2 or table_no == 3:
    #
    #             # Extracting data
    #
    #             rows = table.xpath('.//tr[number(string(td[last()])) = number(string(td[last()]))]')
    #             for i, row in enumerate(rows):
    #
    #                 data = row.xpath('./td//text()').getall()
    #                 # Initialize table_name
    #                 table_name = None
    #
    #                 element = data[0]
    #
    #                 # Check for partial matches using regex (case-insensitive)
    #                 for category in categories:
    #                     if re.search(rf'\b{re.escape(element)}\b', category, re.IGNORECASE):
    #                         table_name = category
    #                         print(f"Match found: {table_name}")
    #                         break
    #
    #                 # handling edge cases
    #                 if re.search(r'^Salary', element, re.IGNORECASE):
    #                     table_name = "Salary/Wages (D-I)"
    #
    #                 if table_name is None:
    #                     table_name = "Part-time/Contractual/Outsourced Employees"
    #
    #                 td_count = len(data)
    #
    #                 # Check if row has more than 6 <td> elements
    #                 if td_count > 4:
    #                     results[Category][table_name].update({
    #                         "Expectation for 2 qtr ahead - Net Res": float(data[3]),
    #                         "Expectation for 3 qtr ahead - Net Res": float(data[4])
    #                     })
    #
    #
    #     self.check_duplicate_date(extracted_date)
    #
    #     json_data = {
    #         "date": extracted_date,
    #         "data": results
    #     }
    #
    #     json_line = json.dumps(json_data)
    #     print(json_line)
    #
    #
    #     with open(self.jsonl_file_path, "a", encoding="utf-8") as file:
    #         file.write(json_line + "\n")
    #
    #     print(f"Data appended to {self.jsonl_file_path}")
    #
    #
    # def convert_quarter_date(self,q_str):
    #     try:
    #         if q_str.startswith("Q") and ":" in q_str:
    #             parts = q_str.split(":")
    #             quarter_num = parts[0][1]  # e.g. '4' from 'Q4'
    #             year_part = parts[1].split("-")[0]  # e.g. '2020' from '2020-21'
    #             if (quarter_num == '4'):
    #                 year_part = int(year_part) + 1
    #             mapping = {
    #                 "1": "06-01",
    #                 "2": "09-01",
    #                 "3": "12-01",
    #                 "4": "03-01"
    #             }
    #             md = mapping.get(quarter_num, "01-01")
    #             return f"20{year_part}-{md}"
    #     except:
    #         pass
    #     # Return original if something doesn't match
    #     return q_str
    #
    #
    # def check_duplicate_date(self, extracted_date):
    #     try:
    #         with open(self.jsonl_file_path, 'r', encoding='utf-8') as file:
    #             for line in file:
    #                 data_point = json.loads(line.strip())
    #                 if data_point.get('date') == extracted_date:
    #                     raise CloseSpider(f'Data with date {extracted_date} is already present. Closing spider.')
    #     except FileNotFoundError:
    #         self.logger.warning(f'File not found: {self.jsonl_file_path}. Proceeding as new file.')
    #     except json.JSONDecodeError as e:
    #         self.logger.error(f'Error reading JSONL file: {e}')
    #
    # def parse_item(self, response):
    #     item = {}
    #     #item["domain_id"] = response.xpath('//input[@id="sid"]/@value').get()
    #     #item["name"] = response.xpath('//div[@id="name"]').get()
    #     #item["description"] = response.xpath('//div[@id="description"]').get()
    #     return item
