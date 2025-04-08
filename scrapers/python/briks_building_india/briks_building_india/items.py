# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class BriksBuildingIndiaItem(scrapy.Item):
    files = scrapy.Field()
    file_urls = scrapy.Field()
