"""
dag_ndvi.py — Airflow DAG for Google Earth Engine NDVI pipeline.

Schedule: Monthly (5th of each month at 2 AM)
Tasks: authenticate_gee → compute_ndvi → aggregate_to_wards → load_to_raw → log_pipeline_run
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
    dag_id="ndvi_ingestion",
    default_args=default_args,
    description="Compute monthly NDVI from Sentinel-2 via GEE, aggregate to ward polygons",
    schedule="0 2 5 * *",  # 5th of each month at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "ndvi", "gee", "remote-sensing"],
)
def ndvi_ingestion():
    """NDVI remote sensing ingestion pipeline."""

    @task()
    def start_pipeline_run(**context):
        from scripts.db_utils import log_pipeline_start
        return log_pipeline_start(
            dag_id="ndvi_ingestion",
            task_id="full_pipeline",
            execution_date=context["execution_date"],
        )

    @task()
    def authenticate_gee():
        """Authenticate with Google Earth Engine."""
        from scripts.geo.ndvi_pipeline import authenticate_gee as gee_auth
        gee_auth()
        return True

    @task()
    def compute_and_aggregate(gee_authenticated: bool,
                               **context) -> list[dict]:
        """
        Compute NDVI and aggregate to wards.
        Processes the previous month's data.
        """
        from scripts.geo.ndvi_pipeline import aggregate_ndvi_from_gee

        exec_date = context["execution_date"]
        # Process previous month
        if exec_date.month == 1:
            target_year = exec_date.year - 1
            target_month = 12
        else:
            target_year = exec_date.year
            target_month = exec_date.month - 1

        ward_geojson = "/opt/airflow/data/ward_boundaries.geojson"

        records = aggregate_ndvi_from_gee(
            ward_geojson_path=ward_geojson,
            year=target_year,
            month=target_month,
        )

        logger.info(
            f"Computed NDVI for {len(records)} wards "
            f"({target_year}-{target_month:02d})"
        )
        return records

    @task()
    def load_to_raw(records: list[dict]) -> int:
        """Load NDVI records into raw.ndvi."""
        from scripts.geo.ndvi_pipeline import get_prepare_rows
        from scripts.db_utils import upsert_rows

        if not records:
            logger.warning("No NDVI records to load")
            return 0

        columns, rows = get_prepare_rows(records)
        loaded = upsert_rows(
            table="raw.ndvi",
            columns=columns,
            rows=rows,
            conflict_columns=["ward_id", "year", "month"],
            update_columns=[
                "mean_ndvi", "median_ndvi", "min_ndvi", "max_ndvi",
                "std_ndvi", "pixel_count"
            ],
        )
        return loaded

    @task()
    def finalize_pipeline(run_id: str, records_loaded: int):
        from scripts.db_utils import log_pipeline_success, log_pipeline_failure

        if records_loaded > 0:
            log_pipeline_success(run_id=run_id, records_loaded=records_loaded)
        else:
            log_pipeline_failure(
                run_id=run_id,
                error_message="No NDVI records loaded",
            )

    run_id = start_pipeline_run()
    gee_auth = authenticate_gee()
    records = compute_and_aggregate(gee_authenticated=gee_auth)
    loaded = load_to_raw(records)
    finalize_pipeline(run_id=run_id, records_loaded=loaded)


ndvi_ingestion()
