"""
PostgreSQL pipeline for BBMP Sahaaya spider.

Inserts complaint items into raw.bbmp_complaints with
ON CONFLICT handling for deduplication.
"""

import os
import logging

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


class PostgresPipeline:
    """Pipeline that writes complaint items to PostgreSQL."""

    def __init__(self):
        self.conn = None
        self.cursor = None

    def open_spider(self, spider):
        """Open DB connection when spider starts."""
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "lss_warehouse"),
            user=os.getenv("POSTGRES_USER", "lss_user"),
            password=os.getenv("POSTGRES_PASSWORD", "lss_password"),
        )
        self.cursor = self.conn.cursor()
        logger.info("PostgresPipeline: DB connection opened")

    def close_spider(self, spider):
        """Close DB connection when spider finishes."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()
        logger.info("PostgresPipeline: DB connection closed")

    def process_item(self, item, spider):
        """Insert a complaint item into raw.bbmp_complaints."""
        try:
            self.cursor.execute("""
                INSERT INTO raw.bbmp_complaints
                    (complaint_id, category, subcategory, ward_name,
                     ward_number, date_filed, status, description,
                     latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (complaint_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    description = EXCLUDED.description,
                    ingested_at = NOW()
            """, (
                item.get("complaint_id"),
                item.get("category"),
                item.get("subcategory"),
                item.get("ward_name"),
                item.get("ward_number"),
                item.get("date_filed"),
                item.get("status"),
                item.get("description"),
                item.get("latitude"),
                item.get("longitude"),
            ))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert complaint {item.get('complaint_id')}: {e}")

        return item
