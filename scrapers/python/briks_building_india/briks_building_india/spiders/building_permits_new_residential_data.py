# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import json
import os
import re
import time
from datetime import datetime
from rich import print
import pandas as pd
import scrapy
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from scrapy_playwright.page import PageMethod


class BuildingPermitsNewResidentialDataSpider(scrapy.Spider):
    name = "building_permits_new_residential_data"
    allowed_domains = ["briks.gov.in"]
    start_urls = ["https://briks.gov.in/Report/Rept_CityListEnteredRecord.aspx?Moduletype=5"]
    json_file_path = "D:\\Desktop\\financial_data_pipeline\\data\\raw\\briks_building_india\\building_permits_new_residential_data.json"
    ongoing_state = None
    resource_types_to_block = ["image", "font", "media", "stylesheet", "xhr", "script", "other"]
    blocked_domains = [
        "googletagmanager.com","fonts.gstatic.com","doubleclick.net",
        "pubmatic.com", "adform.net", "gammaplatform.com", "pinterest.com",
        "googleadservices.com", "heapanalytics.com", "everesttech.net",
        "quantserve.com", "ctnsnet.com", "tribalfusion.com"
    ]
    custom_settings = {
        # For Playwright settings
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        # For twisted reactor settings
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",

        # 'ITEM_PIPELINES': {
        #     'briks_building_india.pipelines.Backup_Pipeline_briks_building_india': 2
        # },
        # "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        # # Optional: Increase timeout if needed
        # "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000
    }
    blocked_patterns = re.compile(r"(pubmatic|adform|analytics|ads|pinterest|googleadservices|quantserve|tribalfusion|heapanalytics)")

    async def block_requests(self, route):
        request = route.request
        if any(domain in request.url for domain in self.blocked_domains):
            await route.abort()
        elif request.resource_type in self.resource_types_to_block:
            await route.abort()
        elif self.blocked_patterns.search(request.url):
            await route.abort()
        else:
            await route.continue_()


    def start_requests(self):
        self.start_time = time.time()  # Start tracking time
        url = self.start_urls[0]
        yield scrapy.Request(url, callback= self.parse,
                             meta=dict(
                                 playwright=True,
                                 playwright_include_page=True,
                                 playwright_page_methods=[
                                     PageMethod("route", "**/*",
                                                self.block_requests)
                                 ],
                                 errback=self.errback,
                             ))

    async def parse(self, response):
        self.logger.info(f"Visited {response.url}")
        # print(response.text)

        categories_building_permits = ["Sno.", "State Name", "I", "II", "III", "IV", "Total"]

        categories = categories_building_permits

        page = response.meta["playwright_page"]
        years = [
            "2000-2001",
            "2001-2002",
            "2002-2003",
            "2003-2004",
            "2004-2005",
            "2005-2006",
            "2006-2007",
            "2007-2008",
            "2008-2009",
            "2009-2010",
            "2010-2011",
            "2011-2012",
            "2012-2013",
            "2013-2014",
            "2014-2015",
            "2015-2016",
            "2016-2017",
            "2017-2018",
            "2018-2019",
            "2019-2020",
            "2020-2021",
            "2021-2022",
            "2022-2023",
            "2023-2024",
            "2024-2025",
        ]
        json_data = {}

        for year_used_in_site in years:
            year = year_used_in_site.split("-")[1]
            year_to_show = int(year)
            date = f'{year_to_show}-01-01'
            # Select the desired year by its value (e.g., "2023").
            await page.select_option("select#ctl00_ContentPlaceHolder1_ddl_FinYear", value=year_used_in_site)
            await page.wait_for_load_state("networkidle")

            # Click the submit/report button.
            await page.click("input#ctl00_ContentPlaceHolder1_btngo")
            await page.wait_for_load_state("networkidle")

            # Retrieve the rendered HTML content.
            html = await page.content()

            await page.screenshot(
                path='D:\\Desktop\\financial_data_pipeline\\scrapers\\python\\briks_building_india\\images\\screenshot_for_building_permits_new_residential_data.png',
                full_page=True)

            response = Selector(text=html)
            self.logger.info(f"Processing year: {year_used_in_site}")

            results = []
            table =response.xpath('//*[@id="ctl00_ContentPlaceHolder1_grid"]')

            # Extracting data

            rows = table.xpath('./tbody/tr[td]')

            for i, row in enumerate(rows):

                data = row.xpath('./td[2]/table/tbody/tr/td/span/text()').getall()

                record_no_raw = row.xpath('./td[1]/text()').get().strip().replace(r"\"","")

                state_text = row.xpath('./td[2]/table/tbody/tr/td/a/text()').get()

                # Insert the state_text at the beginning of the data list
                data.insert(0, state_text)

                if record_no_raw is not None:
                    data.insert(0, record_no_raw)
                    print(record_no_raw)
                else:
                    print("record_no_raw is none")

                # print(data)
                #
                # if i>30:
                #     break

                td_count = len(data)

                # Check if row has more than 6 <td> elements
                if td_count > 6 and state_text is not None:
                    building_permits_dict = dict(zip(categories, data))
                    results.append(building_permits_dict)
                else:
                    self.logger.warning(f"found less than 6 records row or state_text is None at index {i}")
                    continue

            json_data.update({date: results})

        print(json_data.keys())

        # Write the updated data back to the file
        with open(self.json_file_path, "w") as f:
            json.dump(json_data, f, indent=4)

        print(f"Data appended to {self.json_file_path}")

        elapsed_time = time.time() - self.start_time
        self.logger.info(f"Time elapsed so far: {elapsed_time:.2f} seconds")

        ######## Downloading the Excel file with latest data

        # async with page.expect_download() as download_info:
        #     await page.click("input#ctl00_ContentPlaceHolder1_bynexecl")
        # download = await download_info.value
        #
        # # Save the downloaded file to a specific path
        # await download.save_as(f'building_data{year_used_in_site}.xlsx')
        # print("File downloaded")
        # await page.wait_for_load_state("networkidle")

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()
        elapsed_time = time.time() - self.start_time
        self.logger.error(f"Spider failed after {elapsed_time:.2f} seconds")


