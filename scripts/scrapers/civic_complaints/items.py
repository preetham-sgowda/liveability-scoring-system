import scrapy

class CivicComplaintItem(scrapy.Item):
    """Represents a single civic complaint from any city."""
    city = scrapy.Field()
    complaint_id = scrapy.Field()
    category = scrapy.Field()
    subcategory = scrapy.Field()
    ward_name = scrapy.Field()
    ward_number = scrapy.Field()
    date_filed = scrapy.Field()
    status = scrapy.Field()
    description = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
