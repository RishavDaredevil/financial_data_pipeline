# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import scrapy
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose
import re

class RBI_inflation_expectations_survey_excel(scrapy.Item):
    files = scrapy.Field()
    file_urls = scrapy.Field()

def clean_text(value):
    return re.sub(r'[\s::%#*-]+$', '', value.strip())


class RBI_root_page(scrapy.Item):
    Policy_Rates = scrapy.Field()
    Reserve_Ratios = scrapy.Field()
    Exchange_Rates = scrapy.Field()
    Lending_Deposit_Rates = scrapy.Field()
    Market_Trends = scrapy.Field()


class RBIItemLoader(ItemLoader):
    def load_tr_to_dict(self, rows):
        data = {}
        for row in rows:
            key = row.xpath('normalize-space(td[1]/text())').get()
            value = row.xpath('normalize-space(td[2])').get()
            if key and value:
                data[key] = clean_text(value).replace(':', '').strip()
        return data

class RbiScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
