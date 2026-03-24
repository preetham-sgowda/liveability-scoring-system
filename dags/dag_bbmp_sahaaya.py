"""
dag_bbmp_sahaaya.py — Airflow DAG for BBMP Sahaaya complaints.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow")

@dag(
    dag_id="bbmp_sahaaya_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["lss", "complaints"]
)
def bbmp_sahaaya_ingestion():
    @task()
    def run_sahaaya_etl():
        from scripts.db_utils import log_pipeline_start, log_pipeline_success, log_pipeline_failure
        import subprocess

        run_id = log_pipeline_start(dag_id="bbmp_sahaaya_ingestion", task_id="etl")
        try:
            # Run Scrapy via subprocess
            res = subprocess.run(
                ["scrapy", "crawl", "sahaaya"],
                cwd="/opt/airflow/scripts/scrapers/bbmp_sahaaya",
                capture_output=True, text=True
            )
            if res.returncode == 0:
                log_pipeline_success(run_id, 0) # Count logic handled in pipeline
            else:
                log_pipeline_failure(run_id, res.stderr)
        except Exception as e:
            log_pipeline_failure(run_id, str(e))

    run_sahaaya_etl()

bbmp_sahaaya_ingestion()
