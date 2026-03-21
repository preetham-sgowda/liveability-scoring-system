"""
dag_property_prices.py — Airflow DAG for property price scraping.

Schedule: Weekly (every Sunday at 4 AM)
Tasks: run_scraper → validate_data → load_to_raw → log_pipeline_run

NOTE: Property prices are used as a VALIDATION LABEL only, not an ML input.
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "sanjana",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="property_prices_ingestion",
    default_args=default_args,
    description="Scrape Bengaluru property prices (validation label only)",
    schedule="0 4 * * 0",  # Every Sunday at 4 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "property", "scraper", "validation-label"],
)
def property_prices_ingestion():
    """Property price scraping pipeline."""

    @task()
    def start_pipeline_run(**context):
        from scripts.db_utils import log_pipeline_start
        return log_pipeline_start(
            dag_id="property_prices_ingestion",
            task_id="full_pipeline",
            execution_date=context["execution_date"],
        )

    @task()
    def scrape_properties(source: str = "magicbricks",
                          max_pages: int = 2) -> list[dict]:
        """Scrape property listings from the specified source."""
        from scripts.scrapers.property_scraper import PropertyScraper

        scraper = PropertyScraper()
        listings = scraper.scrape_all_localities(
            source=source, max_pages=max_pages
        )
        logger.info(f"Scraped {len(listings)} property listings from {source}")
        return listings

    @task()
    def validate_and_clean(listings: list[dict]) -> list[dict]:
        """Validate and clean property listings."""
        clean = []
        for listing in listings:
            # Must have price or price_sqft
            if not listing.get("price_total") and not listing.get("price_sqft"):
                continue

            # Price sanity check (Bengaluru range: ₹2K - ₹50K per sqft)
            if listing.get("price_sqft"):
                if listing["price_sqft"] < 1000 or listing["price_sqft"] > 100000:
                    logger.debug(
                        f"Skipping outlier: ₹{listing['price_sqft']}/sqft "
                        f"in {listing.get('locality')}"
                    )
                    continue

            clean.append(listing)

        logger.info(f"Validated: {len(clean)}/{len(listings)} listings passed")
        return clean

    @task()
    def load_to_raw(listings: list[dict]) -> int:
        """Load property listings to raw.property_prices."""
        from scripts.scrapers.property_scraper import get_prepare_rows
        from scripts.db_utils import bulk_insert

        if not listings:
            return 0

        columns, rows = get_prepare_rows(listings)
        loaded = bulk_insert("raw.property_prices", columns, rows)
        return loaded

    @task()
    def finalize_pipeline(run_id: str, records_loaded: int):
        from scripts.db_utils import log_pipeline_success, log_pipeline_failure

        if records_loaded > 0:
            log_pipeline_success(run_id=run_id, records_loaded=records_loaded)
        else:
            log_pipeline_failure(
                run_id=run_id,
                error_message="No property listings loaded",
            )

    run_id = start_pipeline_run()
    raw_listings = scrape_properties()
    clean_listings = validate_and_clean(raw_listings)
    loaded = load_to_raw(clean_listings)
    finalize_pipeline(run_id=run_id, records_loaded=loaded)


property_prices_ingestion()
