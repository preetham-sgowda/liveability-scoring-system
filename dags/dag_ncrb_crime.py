"""
dag_ncrb_crime.py — Airflow DAG for NCRB Crime PDF ingestion.

Schedule: Manual trigger (NCRB publishes annually)
Tasks: download_pdf → parse_pdf → load_to_raw → log_pipeline_run
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "sanjana",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="ncrb_crime_ingestion",
    default_args=default_args,
    description="Parse NCRB Crime in India PDFs and load to raw.ncrb_crime",
    schedule=None,  # Manual trigger — NCRB publishes annually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "crime", "ncrb"],
)
def ncrb_crime_ingestion():
    """NCRB Crime PDF ingestion pipeline."""

    @task()
    def start_pipeline_run(**context):
        """Log pipeline start to raw.pipeline_runs."""
        from scripts.db_utils import log_pipeline_start

        run_id = log_pipeline_start(
            dag_id="ncrb_crime_ingestion",
            task_id="full_pipeline",
            execution_date=context["execution_date"],
        )
        return run_id

    @task()
    def discover_pdfs(data_dir: str = "/opt/airflow/data/ncrb") -> list[dict]:
        """
        Discover NCRB PDF files to process.
        Returns list of {path, year} dicts.
        """
        pdf_dir = Path(data_dir)
        if not pdf_dir.exists():
            logger.warning(f"NCRB data directory not found: {data_dir}")
            return []

        pdfs = []
        for pdf_file in sorted(pdf_dir.glob("*.pdf")):
            # Try to extract year from filename
            import re
            year_match = re.search(r"(20\d{2})", pdf_file.name)
            if year_match:
                pdfs.append({
                    "path": str(pdf_file),
                    "year": int(year_match.group(1)),
                })
            else:
                logger.warning(f"Cannot determine year for: {pdf_file.name}")

        logger.info(f"Discovered {len(pdfs)} NCRB PDFs")
        return pdfs

    @task()
    def parse_and_load(pdf_info: dict, run_id: str) -> dict:
        """
        Parse a single NCRB PDF and load records to raw.ncrb_crime.
        Returns stats dict.
        """
        from scripts.parsers.ncrb_pdf_parser import (
            parse_ncrb_pdf, get_prepare_rows
        )
        from scripts.db_utils import delete_and_insert

        pdf_path = pdf_info["path"]
        year = pdf_info["year"]

        logger.info(f"Processing: {pdf_path} (year={year})")

        # Parse PDF
        records = parse_ncrb_pdf(pdf_path, year=year, target_city="bengaluru")

        if not records:
            logger.warning(f"No records extracted from {pdf_path}")
            return {"file": pdf_path, "year": year, "records": 0}

        # Prepare and load
        columns, rows = get_prepare_rows(records)
        loaded = delete_and_insert(
            table="raw.ncrb_crime",
            columns=columns,
            rows=rows,
            where_clause="year = %s AND city = %s",
            where_params=(year, "Bengaluru"),
        )

        return {"file": pdf_path, "year": year, "records": loaded}

    @task()
    def finalize_pipeline(run_id: str, results: list[dict]):
        """Log pipeline completion."""
        from scripts.db_utils import log_pipeline_success, log_pipeline_failure

        total_records = sum(r.get("records", 0) for r in results)

        if total_records > 0:
            log_pipeline_success(
                run_id=run_id,
                records_loaded=total_records,
                metadata={"files_processed": len(results), "results": results},
            )
            logger.info(f"Pipeline complete: {total_records} records loaded")
        else:
            log_pipeline_failure(
                run_id=run_id,
                error_message="No records extracted from any PDF",
                metadata={"results": results},
            )
            logger.error("Pipeline completed with 0 records")

    # DAG flow
    run_id = start_pipeline_run()
    pdfs = discover_pdfs()
    results = parse_and_load.expand(pdf_info=pdfs, run_id=[run_id])
    finalize_pipeline(run_id=run_id, results=results)


ncrb_crime_ingestion()
