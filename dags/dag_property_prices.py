"""
dag_property_prices.py — Airflow DAG for Property prices.
"""
from airflow.decorators import dag, task
from datetime import datetime
import sys

sys.path.append("/opt/airflow")

@dag(
    dag_id="property_prices_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lss", "property"]
)
def property_prices_ingestion():
    @task()
    def run_property_etl():
        from scripts.db_utils import log_pipeline_start, log_pipeline_success, log_pipeline_failure
        from scripts.scrapers.property_scraper import PropertyScraper, get_prepare_rows
        from scripts.db_utils import delete_and_insert

        run_id = log_pipeline_start(dag_id="property_prices_ingestion", task_id="etl")
        try:
            scraper = PropertyScraper()
            listings = scraper.scrape_all_localities(source="magicbricks", max_pages=1)
            if not listings:
                log_pipeline_failure(run_id, "No listings scraped")
                return

            cols, rows = get_prepare_rows(listings)
            loaded = delete_and_insert(
                table="raw.property_prices", columns=cols, rows=rows,
                where_clause="source = 'magicbricks'", where_params=()
            )
            log_pipeline_success(run_id, loaded)
        except Exception as e:
            log_pipeline_failure(run_id, str(e))

    run_property_etl()

property_prices_ingestion()
