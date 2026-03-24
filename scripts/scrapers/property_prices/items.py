import scrapy

class PropertyListingItem(scrapy.Item):
    """Represents a property listing from MagicBricks or 99acres."""
    city = scrapy.Field()
    locality = scrapy.Field()
    source = scrapy.Field()
    price_total = scrapy.Field()
    price_sqft = scrapy.Field()
    area_sqft = scrapy.Field()
    bedrooms = scrapy.Field()
    property_type = scrapy.Field()
    listing_date = scrapy.Field()
