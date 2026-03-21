"""
dag_bbmp_sahaaya.py — Airflow DAG for BBMP Sahaaya complaint scraping.

Schedule: Weekly (every Monday at 6 AM IST)
Tasks: run_scrapy_spider → validate_counts → log_pipeline_run
"""

import sys
import logging
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "sanjana",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="bbmp_sahaaya_ingestion",
    default_args=default_args,
    description="Scrape BBMP Sahaaya civic complaints weekly",
    schedule="0 6 * * 1",  # Every Monday 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "complaints", "bbmp", "scraper"],
)
def bbmp_sahaaya_ingestion():
    """BBMP Sahaaya complaints scraping pipeline."""

    @task()
    def start_pipeline_run(**context):
        from scripts.db_utils import log_pipeline_start
        return log_pipeline_start(
            dag_id="bbmp_sahaaya_ingestion",
            task_id="full_pipeline",
            execution_date=context["execution_date"],
        )

    @task()
    def run_scrapy_spider() -> dict:
        """
        Run the BBMP Sahaaya Scrapy spider as a subprocess.
        Returns scrape statistics.
        """
        spider_dir = "/opt/airflow/scripts/scrapers/bbmp_sahaaya"

        result = subprocess.run(
            ["scrapy", "crawl", "sahaaya", "-s", "LOG_LEVEL=INFO"],
            cwd=spider_dir,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
        )

        if result.returncode != 0:
            logger.error(f"Scrapy failed: {result.stderr}")
            raise Exception(f"Scrapy spider failed: {result.stderr[:500]}")

        # Parse item count from Scrapy output
        import re
        item_match = re.search(r"'item_scraped_count':\s*(\d+)", result.stdout)
        items_scraped = int(item_match.group(1)) if item_match else 0

        logger.info(f"Scrapy finished: {items_scraped} items scraped")
        return {"items_scraped": items_scraped, "output": result.stdout[-500:]}

    @task()
    def validate_counts(scrape_stats: dict) -> dict:
        """Validate scrape results and compute summary statistics."""
        from scripts.db_utils import get_db_connection

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Total records in raw table
                cur.execute("SELECT COUNT(*) FROM raw.bbmp_complaints")
                total = cur.fetchone()[0]

                # Records ingested today
                cur.execute("""
                    SELECT COUNT(*) FROM raw.bbmp_complaints
                    WHERE ingested_at >= CURRENT_DATE
                """)
                today = cur.fetchone()[0]

                # Distinct wards
                cur.execute("""
                    SELECT COUNT(DISTINCT ward_name) FROM raw.bbmp_complaints
                    WHERE ward_name IS NOT NULL
                """)
                wards = cur.fetchone()[0]

        stats = {
            "total_records": total,
            "records_today": today,
            "distinct_wards": wards,
            "items_scraped": scrape_stats.get("items_scraped", 0),
        }

        logger.info(f"Validation stats: {stats}")
        return stats

    @task()
    def finalize_pipeline(run_id: str, stats: dict):
        from scripts.db_utils import log_pipeline_success, log_pipeline_failure

        if stats.get("items_scraped", 0) > 0:
            log_pipeline_success(
                run_id=run_id,
                records_loaded=stats["items_scraped"],
                metadata=stats,
            )
        else:
            log_pipeline_failure(
                run_id=run_id,
                error_message="No items scraped",
                metadata=stats,
            )

    run_id = start_pipeline_run()
    scrape_stats = run_scrapy_spider()
    stats = validate_counts(scrape_stats)
    finalize_pipeline(run_id=run_id, stats=stats)


bbmp_sahaaya_ingestion()
