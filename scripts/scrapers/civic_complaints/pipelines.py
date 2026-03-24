import logging
from scripts.db_utils import upsert_rows

logger = logging.getLogger(__name__)

class PostgresPipeline:
    """Pipeline to store scraped complaints into PostgreSQL."""

    def process_item(self, item, spider):
        table = "raw.bbmp_complaints" if item['city'] == 'Bengaluru' else "raw.civic_complaints"
        # Note: We need a raw.civic_complaints table for non-Bengaluru cities if we want to generalize.
        # However, the current schema has raw.bbmp_complaints.
        # Let's ensure the schema is generalized or use city-specific tables.
        
        # Recommendation: Use a single raw.civic_complaints table for all cities.
        # I'll update the schema to include raw.civic_complaints.
        
        cols = [
            "city", "complaint_id", "category", "subcategory", "ward_name", 
            "ward_number", "date_filed", "status", "description", 
            "latitude", "longitude"
        ]
        row = (
            item.get("city"),
            item.get("complaint_id"),
            item.get("category"),
            item.get("subcategory"),
            item.get("ward_name"),
            item.get("ward_number"),
            item.get("date_filed"),
            item.get("status"),
            item.get("description"),
            item.get("latitude"),
            item.get("longitude")
        )

        try:
            upsert_rows(
                table="raw.civic_complaints",
                columns=cols,
                rows=[row],
                conflict_columns=["city", "complaint_id"],
                update_columns=["status", "description", "latitude", "longitude"]
            )
        except Exception as e:
            logger.error(f"Error inserting item {item.get('complaint_id')}: {e}")
            
        return item
