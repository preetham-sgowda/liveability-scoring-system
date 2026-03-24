"""
dag_civic_complaints.py — Unified Airflow DAG for multi-city civic complaints.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
import subprocess

sys.path.append("/opt/airflow")

@dag(
    dag_id="civic_complaints_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["lss", "complaints"]
)
def civic_complaints_ingestion():
    
    cities = ["bengaluru", "mumbai", "delhi"]

    @task(task_id="run_city_scraper")
    def run_scraper(city: str):
        from scripts.db_utils import log_pipeline_start, log_pipeline_success, log_pipeline_failure
        
        run_id = log_pipeline_start(dag_id="civic_complaints_ingestion", task_id=f"scrape_{city}")
        try:
            res = subprocess.run(
                ["scrapy", "crawl", city],
                cwd="/opt/airflow/scripts/scrapers/civic_complaints",
                capture_output=True, text=True
            )
            if res.returncode == 0:
                log_pipeline_success(run_id, 0)
            else:
                log_pipeline_failure(run_id, res.stderr)
        except Exception as e:
            log_pipeline_failure(run_id, str(e))

    for city in cities:
        run_scraper.override(task_id=f"scrape_{city}")(city)

civic_complaints_ingestion()
