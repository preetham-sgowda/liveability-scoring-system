"""
dag_gtfs_transit.py — Airflow DAG for GTFS transit data.
"""
from airflow.decorators import dag, task
from datetime import datetime
import sys

sys.path.append("/opt/airflow")

@dag(
    dag_id="gtfs_transit_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["lss", "transit", "gtfs"]
)
def gtfs_transit_ingestion():
    
    config = [
        {"city": "Bengaluru", "agencies": [("bmtc", "BMTC"), ("bmrcl", "BMRCL")]},
        {"city": "Mumbai", "agencies": [("best", "BEST"), ("mumbai_metro", "MMRDA")]},
        {"city": "Delhi", "agencies": [("dtc", "DTC"), ("delhi_metro", "DMRC")]},
    ]

    @task(task_id="run_agency_gtfs_etl")
    def run_gtfs_etl(city: str, agency_slug: str, source_name: str):
        from scripts.db_utils import log_pipeline_start, log_pipeline_success, log_pipeline_failure
        from scripts.loaders.gtfs_loader import parse_gtfs_stops, compute_route_frequency, prepare_stops_for_load, get_prepare_rows
        from scripts.db_utils import delete_and_insert
        from pathlib import Path

        run_id = log_pipeline_start(dag_id="gtfs_transit_ingestion", task_id=f"etl_{agency_slug}")
        gtfs_dir = f"/opt/airflow/data/gtfs/{agency_slug}"
        
        if not Path(gtfs_dir).exists():
            log_pipeline_failure(run_id, f"GTFS dir {gtfs_dir} not found")
            return

        try:
            stops_df = parse_gtfs_stops(gtfs_dir, source=source_name)
            freq_df = compute_route_frequency(gtfs_dir)
            final_df = prepare_stops_for_load(stops_df, freq_df)
            cols, rows = get_prepare_rows(final_df)
            loaded = delete_and_insert(
                table="raw.gtfs_stops", columns=cols, rows=rows,
                where_clause="source = %s", where_params=(source_name,)
            )
            log_pipeline_success(run_id, loaded)
        except Exception as e:
            log_pipeline_failure(run_id, str(e))

    for city_conf in config:
        for slug, name in city_conf["agencies"]:
            run_gtfs_etl.override(task_id=f"etl_{slug}")(city_conf["city"], slug, name)

gtfs_transit_ingestion()
