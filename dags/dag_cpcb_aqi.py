"""
dag_cpcb_aqi.py — Airflow DAG for CPCB AQI data ingestion.

Schedule: Daily at 7 AM IST
Tasks: fetch_stations → fetch_daily_data → load_to_raw → log_pipeline_run
"""

import sys
import logging
from datetime import datetime, timedelta, date

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
    dag_id="cpcb_aqi_ingestion",
    default_args=default_args,
    description="Fetch daily AQI readings from CPCB for Bengaluru stations",
    schedule="0 7 * * *",  # Daily at 7 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "aqi", "cpcb", "api"],
)
def cpcb_aqi_ingestion():
    """CPCB AQI daily ingestion pipeline."""

    @task()
    def start_pipeline_run(**context):
        from scripts.db_utils import log_pipeline_start
        return log_pipeline_start(
            dag_id="cpcb_aqi_ingestion",
            task_id="full_pipeline",
            execution_date=context["execution_date"],
        )

    @task()
    def fetch_daily_data(**context) -> list[dict]:
        """
        Fetch AQI data for yesterday (data availability lag).
        """
        from scripts.api_clients.cpcb_aqi_client import CpcbAqiClient

        # Fetch yesterday's data (typical 1-day lag)
        target_date = (
            context["execution_date"].date() - timedelta(days=1)
        )

        client = CpcbAqiClient()
        readings = client.fetch_daily_readings(target_date)

        logger.info(f"Fetched {len(readings)} AQI readings for {target_date}")
        return readings

    @task()
    def load_to_raw(readings: list[dict]) -> int:
        """Load AQI readings into raw.cpcb_aqi."""
        from scripts.api_clients.cpcb_aqi_client import get_prepare_rows
        from scripts.db_utils import delete_and_insert

        if not readings:
            logger.warning("No AQI readings to load")
            return 0

        columns, rows = get_prepare_rows(readings)
        target_date = readings[0]["date"]

        loaded = delete_and_insert(
            table="raw.cpcb_aqi",
            columns=columns,
            rows=rows,
            where_clause="date = %s AND city = %s",
            where_params=(target_date, "Bengaluru"),
        )
        return loaded

    @task()
    def finalize_pipeline(run_id: str, records_loaded: int):
        from scripts.db_utils import log_pipeline_success, log_pipeline_failure

        if records_loaded > 0:
            log_pipeline_success(
                run_id=run_id,
                records_loaded=records_loaded,
            )
        else:
            log_pipeline_failure(
                run_id=run_id,
                error_message="No AQI readings loaded",
            )

    run_id = start_pipeline_run()
    readings = fetch_daily_data()
    records_loaded = load_to_raw(readings)
    finalize_pipeline(run_id=run_id, records_loaded=records_loaded)


cpcb_aqi_ingestion()
