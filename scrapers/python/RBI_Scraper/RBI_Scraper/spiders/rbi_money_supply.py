# AUTOTHROTTLE_ENABLED = True by default see if you want to change it
# Add more user agents in middleware if you want
import scrapy

class RbiMoneySupplySpider(scrapy.Spider):
    name = "rbi_money_supply"
    allowed_domains = ["website.rbi.org.in"]
    start_urls = ["https://website.rbi.org.in/web/rbi/publications/rbi-bulletin"]

    def parse(self, response):
        pass
