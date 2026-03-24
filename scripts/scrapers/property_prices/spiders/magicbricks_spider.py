import scrapy
from property_prices.items import PropertyListingItem
from datetime import date

class MagicBricksSpider(scrapy.Spider):
    name = "magicbricks"
    
    localities = {
        "Bengaluru": ["whitefield", "koramangala", "indiranagar"],
        "Mumbai": ["bandra-west", "andheri-east", "worli"],
        "Delhi": ["dwarka", "saket", "vasant-kunj"]
    }

    def start_requests(self):
        for city, locs in self.localities.items():
            for loc in locs:
                url = f"https://www.magicbricks.com/property-for-sale/residential-real-estate?cityName={city.replace(' ', '%20')}&localty={loc}"
                yield scrapy.Request(url, callback=self.parse, meta={'city': city, 'locality': loc})

    def parse(self, response):
        city = response.meta['city']
        loc = response.meta['locality']
        for card in response.css(".mb-srp__card"):
            item = PropertyListingItem()
            item['city'] = city
            item['locality'] = loc
            item['source'] = "magicbricks"
            # Extraction logic... (Simplified for this task)
            yield item
