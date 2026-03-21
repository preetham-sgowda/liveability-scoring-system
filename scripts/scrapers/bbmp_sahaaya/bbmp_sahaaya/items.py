"""
BBMP Sahaaya complaint items definition.
"""

import scrapy


class ComplaintItem(scrapy.Item):
    """Represents a single BBMP Sahaaya civic complaint."""
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
