"""
dag_ndvi.py — Airflow DAG for NDVI raster data.
"""
from airflow.decorators import dag, task
from datetime import datetime
import sys

sys.path.append("/opt/airflow")

@dag(
    dag_id="ndvi_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 5 * *",
    catchup=False,
    tags=["lss", "ndvi", "gee"]
)
def ndvi_ingestion():
    cities = ["Bengaluru", "Mumbai", "Delhi"]

    @task(task_id="run_city_ndvi_etl")
    def run_ndvi_etl(city: str):
        from scripts.db_utils import log_pipeline_start, log_pipeline_success, log_pipeline_failure
        from scripts.geo.ndvi_pipeline import compute_ndvi_composite, export_ndvi_raster, aggregate_ndvi_to_wards, get_prepare_rows
        from scripts.db_utils import delete_and_insert
        from datetime import datetime
        import os

        run_id = log_pipeline_start(dag_id="ndvi_ingestion", task_id=f"etl_{city}")
        # Process previous month
        now = datetime.now()
        year = now.year if now.month > 1 else now.year - 1
        month = now.month - 1 if now.month > 1 else 12
        
        try:
            from scripts.geo.ndvi_pipeline import authenticate_gee
            authenticate_gee()
            
            ndvi_img = compute_ndvi_composite(year, month, city=city)
            raster_path = export_ndvi_raster(ndvi_img, year, month, "/opt/airflow/data/ndvi", city=city)
            
            # Note: Need ward GeoJSON per city. Assuming paths like /opt/airflow/data/wards/{city}_wards.geojson
            ward_path = f"/opt/airflow/data/wards/{city.lower()}_wards.geojson"
            if not os.path.exists(ward_path):
                 # Fallback to general wards if city-specific doesn't exist (not ideal)
                 ward_path = "/opt/airflow/data/wards/wards.geojson"
            
            records = aggregate_ndvi_to_wards(raster_path, ward_path, year, month)
            cols, rows = get_prepare_rows(records)
            loaded = delete_and_insert(
                table="raw.ndvi", columns=cols, rows=rows,
                where_clause="year = %s AND month = %s AND ward_id IN (SELECT ward_id FROM raw.ward_boundaries WHERE city = %s)", 
                where_params=(year, month, city)
            )
            log_pipeline_success(run_id, loaded)
        except Exception as e:
            log_pipeline_failure(run_id, str(e))

    for city in cities:
        run_ndvi_etl.override(task_id=f"etl_{city}")(city)

ndvi_ingestion()
