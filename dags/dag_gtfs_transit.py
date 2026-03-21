"""
dag_gtfs_transit.py — Airflow DAG for GTFS transit data ingestion.

Schedule: Monthly (1st of each month at 3 AM)
Tasks: download_gtfs → parse_stops → compute_frequency → load_to_raw → log_pipeline_run
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
    dag_id="gtfs_transit_ingestion",
    default_args=default_args,
    description="Parse GTFS feeds and load transit data (bus stops, route frequency)",
    schedule="0 3 1 * *",  # 1st of each month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "transit", "gtfs", "bmtc"],
)
def gtfs_transit_ingestion():
    """GTFS transit data ingestion pipeline."""

    @task()
    def start_pipeline_run(**context):
        from scripts.db_utils import log_pipeline_start
        return log_pipeline_start(
            dag_id="gtfs_transit_ingestion",
            task_id="full_pipeline",
            execution_date=context["execution_date"],
        )

    @task()
    def parse_stops(gtfs_dir: str = "/opt/airflow/data/gtfs_sample") -> dict:
        """Parse GTFS stops.txt and return stats."""
        from scripts.loaders.gtfs_loader import parse_gtfs_stops
        import json

        df = parse_gtfs_stops(gtfs_dir, source="BMTC")
        stats = {
            "total_stops": len(df),
            "gtfs_dir": gtfs_dir,
        }
        logger.info(f"Parsed {len(df)} transit stops")

        # Serialize DataFrame to JSON for XCom
        return {
            "stats": stats,
            "data": df.to_dict(orient="records"),
        }

    @task()
    def compute_frequency(gtfs_dir: str = "/opt/airflow/data/gtfs_sample") -> list[dict]:
        """Compute route frequency per stop."""
        from scripts.loaders.gtfs_loader import compute_route_frequency

        freq_df = compute_route_frequency(gtfs_dir)
        return freq_df.to_dict(orient="records")

    @task()
    def merge_and_load(stops_data: dict, frequency_data: list[dict]) -> int:
        """Merge stops with frequency and load to raw."""
        import pandas as pd
        from scripts.loaders.gtfs_loader import (
            prepare_stops_for_load, get_prepare_rows
        )
        from scripts.db_utils import bulk_insert, get_db_connection

        stops_df = pd.DataFrame(stops_data["data"])
        freq_df = pd.DataFrame(frequency_data)

        merged = prepare_stops_for_load(stops_df, freq_df)
        columns, rows = get_prepare_rows(merged)

        # Clear existing BMTC data and reload
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM raw.gtfs_stops WHERE source = 'BMTC'"
                )
            conn.commit()

        loaded = bulk_insert("raw.gtfs_stops", columns, rows)
        logger.info(f"Loaded {loaded} transit stops")
        return loaded

    @task()
    def finalize_pipeline(run_id: str, records_loaded: int):
        from scripts.db_utils import log_pipeline_success
        log_pipeline_success(run_id=run_id, records_loaded=records_loaded)

    run_id = start_pipeline_run()
    stops = parse_stops()
    frequency = compute_frequency()
    loaded = merge_and_load(stops_data=stops, frequency_data=frequency)
    finalize_pipeline(run_id=run_id, records_loaded=loaded)


gtfs_transit_ingestion()
