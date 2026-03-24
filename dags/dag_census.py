"""
dag_census.py — Airflow DAG for Census CSV data.
"""
from airflow.decorators import dag, task
from datetime import datetime
import sys

sys.path.append("/opt/airflow")

@dag(
    dag_id="census_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lss", "census"]
)
def census_ingestion():
    cities = ["Bengaluru", "Mumbai", "Delhi"]

    @task(task_id="run_city_census_etl")
    def run_census_etl(city: str):
        from scripts.db_utils import log_pipeline_start, log_pipeline_success, log_pipeline_failure
        from scripts.loaders.census_loader import load_census_csv, get_prepare_rows
        from scripts.db_utils import delete_and_insert
        from pathlib import Path

        run_id = log_pipeline_start(dag_id="census_ingestion", task_id=f"etl_{city}")
        # Expected path: /opt/airflow/data/census/{city.lower()}_census.csv
        csv_path = Path(f"/opt/airflow/data/census/{city.lower()}_census.csv")
        
        if not csv_path.exists():
            log_pipeline_failure(run_id, f"Census CSV {csv_path} not found")
            return

        try:
            df = load_census_csv(str(csv_path), year_override=2011)
            # Add city column to the dataframe if not present
            df['city'] = city
            cols, rows = get_prepare_rows(df)
            
            # Update table to raw.census (ensure schema has city column)
            loaded = delete_and_insert(
                table="raw.census", columns=cols, rows=rows,
                where_clause="year = 2011 AND city = %s", where_params=(city,)
            )
            log_pipeline_success(run_id, loaded)
        except Exception as e:
            log_pipeline_failure(run_id, str(e))

    for city in cities:
        run_census_etl.override(task_id=f"etl_{city}")(city)

census_ingestion()
