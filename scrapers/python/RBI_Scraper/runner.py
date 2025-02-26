from scrapy.crawler import CrawlerProcess
from scrapy.utils import project
from RBI_Scraper.spiders.root_page_data import RootPageDataSpider
import os
os.chdir("D:\\Desktop\\financial_data_pipeline\\scrapers\\python\\RBI_Scraper")

process = CrawlerProcess(settings=project.get_project_settings())
process.crawl(RootPageDataSpider)
process.start()

