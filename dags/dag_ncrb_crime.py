"""
dag_ncrb_crime.py — Airflow DAG for NCRB Crime data.
"""
from airflow.decorators import dag, task
from datetime import datetime
import sys
import os

sys.path.append("/opt/airflow")

@dag(
    dag_id="ncrb_crime_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lss", "crime"]
)
def ncrb_crime_ingestion():
    @task()
    def run_ncrb_etl():
        from scripts.db_utils import log_pipeline_start, log_pipeline_success, log_pipeline_failure
        from scripts.parsers.ncrb_pdf_parser import parse_ncrb_pdf, get_prepare_rows
        from scripts.db_utils import delete_and_insert
        from pathlib import Path
        import re

        run_id = log_pipeline_start(dag_id="ncrb_crime_ingestion", task_id="etl")
        data_dir = Path("/opt/airflow/data/ncrb")
        if not data_dir.exists():
            log_pipeline_failure(run_id, "Data directory not found")
            return

        total_records = 0
        target_cities = ["Bengaluru", "Mumbai", "Delhi"]
        
        for pdf_file in sorted(data_dir.glob("*.pdf")):
            year_match = re.search(r"(20\d{2})", pdf_file.name)
            year = int(year_match.group(1)) if year_match else 2022
            
            for city in target_cities:
                try:
                    records = parse_ncrb_pdf(str(pdf_file), year=year, target_city=city)
                    if records:
                        cols, rows = get_prepare_rows(records)
                        loaded = delete_and_insert(
                            table="raw.ncrb_crime", columns=cols, rows=rows,
                            where_clause="year = %s AND city = %s", where_params=(year, city)
                        )
                        total_records += loaded
                except Exception as e:
                    print(f"Error processing {pdf_file.name} for {city}: {e}")

        if total_records > 0:
            log_pipeline_success(run_id, total_records)
        else:
            log_pipeline_failure(run_id, "No records loaded")

    run_ncrb_etl()

ncrb_crime_ingestion()
