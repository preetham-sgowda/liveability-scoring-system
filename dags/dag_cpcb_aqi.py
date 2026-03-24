"""
dag_cpcb_aqi.py — Airflow DAG for CPCB AQI data.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow")

@dag(
    dag_id="cpcb_aqi_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["lss", "aqi"]
)
def cpcb_aqi_ingestion():
    @task()
    def run_aqi_etl():
        from scripts.db_utils import log_pipeline_start, log_pipeline_success, log_pipeline_failure
        from scripts.api_clients.cpcb_aqi_client import CpcbAqiClient, get_prepare_rows
        from scripts.db_utils import upsert_rows
        
        run_id = log_pipeline_start(dag_id="cpcb_aqi_ingestion", task_id="etl")
        cities = ["Bengaluru", "Mumbai", "Delhi"]
        total_loaded = 0
        
        try:
            client = CpcbAqiClient()
            for city in cities:
                readings = client.fetch_latest_readings(city=city)
                if readings:
                    cols, rows = get_prepare_rows(readings)
                    # Use upsert to deduplicate by (station_id, timestamp/date)
                    loaded = upsert_rows(
                        table="raw.cpcb_aqi", 
                        columns=cols, 
                        rows=rows,
                        conflict_columns=["station_id", "date"], 
                        update_columns=["pm25", "pm10", "no2", "so2", "aqi"]
                    )
                    total_loaded += loaded
                    
            log_pipeline_success(run_id, total_loaded)
        except Exception as e:
            log_pipeline_failure(run_id, str(e))

    run_aqi_etl()

cpcb_aqi_ingestion()
