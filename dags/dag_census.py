"""
dag_census.py — Airflow DAG for Census of India CSV ingestion.

Schedule: Manual trigger (static data, loaded once)
Tasks: validate_csv → load_to_raw → log_pipeline_run
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
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="census_ingestion",
    default_args=default_args,
    description="Load Census of India ward-level CSV data",
    schedule=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "census", "static"],
)
def census_ingestion():
    """Census data CSV ingestion pipeline."""

    @task()
    def start_pipeline_run(**context):
        from scripts.db_utils import log_pipeline_start
        return log_pipeline_start(
            dag_id="census_ingestion",
            task_id="full_pipeline",
            execution_date=context["execution_date"],
        )

    @task()
    def validate_csv(csv_path: str = "/opt/airflow/data/census_sample.csv") -> dict:
        """Validate the census CSV and return stats."""
        from scripts.loaders.census_loader import load_census_csv, validate_census_df

        df = load_census_csv(csv_path)
        validation = validate_census_df(df)

        if validation["warnings"]:
            for warn in validation["warnings"]:
                logger.warning(f"Census validation: {warn}")

        return {
            "csv_path": csv_path,
            "total_rows": validation["total_rows"],
            "unique_wards": validation["unique_wards"],
            "years": validation["year_range"],
            "warnings": validation["warnings"],
        }

    @task()
    def load_to_raw(validation: dict) -> int:
        """Load validated census data to raw.census."""
        from scripts.loaders.census_loader import load_census_csv, get_prepare_rows
        from scripts.db_utils import delete_and_insert

        df = load_census_csv(validation["csv_path"])
        columns, rows = get_prepare_rows(df)

        # Idempotent: delete by year(s)
        for year in validation["years"]:
            loaded = delete_and_insert(
                table="raw.census",
                columns=columns,
                rows=[r for r in rows if r[columns.index("year")] == year],
                where_clause="year = %s",
                where_params=(year,),
            )

        logger.info(f"Loaded {len(rows)} census records")
        return len(rows)

    @task()
    def finalize_pipeline(run_id: str, records_loaded: int):
        from scripts.db_utils import log_pipeline_success
        log_pipeline_success(run_id=run_id, records_loaded=records_loaded)

    run_id = start_pipeline_run()
    validation = validate_csv()
    records = load_to_raw(validation)
    finalize_pipeline(run_id=run_id, records_loaded=records)


census_ingestion()
