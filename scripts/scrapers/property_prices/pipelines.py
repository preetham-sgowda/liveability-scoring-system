import logging
from scripts.db_utils import bulk_insert

logger = logging.getLogger(__name__)

class PostgresPipeline:
    def process_item(self, item, spider):
        cols = [
            "source", "locality", "city", "price_total", "price_sqft",
            "area_sqft", "bedrooms", "property_type", "listing_date"
        ]
        row = (
            item.get("source"),
            item.get("locality"),
            item.get("city"),
            item.get("price_total"),
            item.get("price_sqft"),
            item.get("area_sqft"),
            item.get("bedrooms"),
            item.get("property_type"),
            item.get("listing_date")
        )
        try:
            bulk_insert("raw.property_prices", cols, [row])
        except Exception as e:
            logger.error(f"Error inserting property item: {e}")
        return item
