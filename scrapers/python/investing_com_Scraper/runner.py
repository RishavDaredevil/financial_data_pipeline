from scrapy.crawler import CrawlerProcess
from scrapy.utils import project
from investing_com_Scraper.spiders.investing_com_us_bonds_data import InvestingComUsBondsDataSpider
import os
os.chdir("D:/Desktop/financial_data_pipeline/scrapers/python/investing_com_Scraper")

process = CrawlerProcess(settings=project.get_project_settings())
print("Starting the crawler...")
process.crawl(InvestingComUsBondsDataSpider)
process.start()

