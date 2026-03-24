"""
db_utils.py — Shared PostgreSQL connection and pipeline logging utilities.

Used by all ingestion scripts and DAGs for:
  - DB connection management
  - Pipeline run audit logging to raw.pipeline_runs
  - Bulk insert helper
"""

import os
import uuid
import logging
from datetime import datetime
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import execute_values, Json

logger = logging.getLogger(__name__)


def get_connection_params() -> dict:
    """Return DB connection params from environment variables."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5433)),
        "dbname": os.getenv("POSTGRES_DB", "lss_warehouse"),
        "user": os.getenv("POSTGRES_USER", "lss_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "lss_password"),
    }


@contextmanager
def get_db_connection():
    """Context manager for PostgreSQL connections with auto-commit."""
    conn = psycopg2.connect(**get_connection_params())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_db_cursor():
    """Context manager yielding a cursor with auto-commit."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()


def bulk_insert(table: str, columns: list[str], rows: list[tuple],
                page_size: int = 1000) -> int:
    """
    Bulk insert rows into a table using execute_values for performance.

    Args:
        table: Fully qualified table name (e.g., 'raw.ncrb_crime')
        columns: List of column names
        rows: List of tuples to insert
        page_size: Batch size for execute_values

    Returns:
        Number of rows inserted
    """
    if not rows:
        logger.warning(f"No rows to insert into {table}")
        return 0

    cols = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols}) VALUES %s"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=page_size)
        conn.commit()

    logger.info(f"Inserted {len(rows)} rows into {table}")
    return len(rows)


def upsert_rows(table: str, columns: list[str], rows: list[tuple],
                conflict_columns: list[str], update_columns: list[str],
                page_size: int = 1000) -> int:
    """
    Upsert rows with ON CONFLICT DO UPDATE.

    Args:
        table: Fully qualified table name
        columns: All column names being inserted
        rows: List of tuples
        conflict_columns: Columns forming the unique constraint
        update_columns: Columns to update on conflict

    Returns:
        Number of rows upserted
    """
    if not rows:
        return 0

    cols = ", ".join(columns)
    conflict = ", ".join(conflict_columns)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)

    sql = f"""
        INSERT INTO {table} ({cols}) VALUES %s
        ON CONFLICT ({conflict}) DO UPDATE SET {updates}
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=page_size)
        conn.commit()

    logger.info(f"Upserted {len(rows)} rows into {table}")
    return len(rows)


def delete_and_insert(table: str, columns: list[str], rows: list[tuple],
                      where_clause: str, where_params: tuple,
                      page_size: int = 1000) -> int:
    """
    Idempotent load: delete existing rows matching criteria, then insert.

    Args:
        table: Fully qualified table name
        columns: Column names
        rows: Data tuples
        where_clause: SQL WHERE clause (e.g., "year = %s AND city = %s")
        where_params: Parameters for the WHERE clause

    Returns:
        Number of rows inserted
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            delete_sql = f"DELETE FROM {table} WHERE {where_clause}"
            cur.execute(delete_sql, where_params)
            deleted = cur.rowcount
            logger.info(f"Deleted {deleted} existing rows from {table}")

            if rows:
                cols = ", ".join(columns)
                insert_sql = f"INSERT INTO {table} ({cols}) VALUES %s"
                execute_values(cur, insert_sql, rows, page_size=page_size)

        conn.commit()

    logger.info(f"Inserted {len(rows)} rows into {table}")
    return len(rows)


# ── Pipeline Run Logging ──

def ensure_pipeline_runs_table_exists():
    """Create raw.pipeline_runs audit table if missing (or alter schema if needed)."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
                    run_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    dag_id          VARCHAR(100) NOT NULL,
                    task_id         VARCHAR(100),
                    execution_date  TIMESTAMPTZ,
                    status          VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed')),
                    started_at      TIMESTAMPTZ DEFAULT NOW(),
                    finished_at     TIMESTAMPTZ,
                    records_loaded  INTEGER DEFAULT 0,
                    error_message   TEXT,
                    metadata        JSONB
                );
            """)
            # Ensure columns exist for backward compatibility with older schema
            cur.execute("""
                ALTER TABLE raw.pipeline_runs
                    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ DEFAULT NOW(),
                    ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS records_loaded INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS error_message TEXT,
                    ADD COLUMN IF NOT EXISTS metadata JSONB;
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag
                    ON raw.pipeline_runs (dag_id, started_at DESC);
            """)
        conn.commit()


def log_pipeline_start(dag_id: str, task_id: str = None,
                       execution_date: datetime = None) -> str:
    """
    Log the start of a pipeline run to raw.pipeline_runs.

    Returns:
        run_id (UUID string)
    """
    ensure_pipeline_runs_table_exists()
    run_id = str(uuid.uuid4())
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw.pipeline_runs
                    (run_id, dag_id, task_id, execution_date, status, started_at)
                VALUES (%s, %s, %s, %s, 'running', NOW())
            """, (run_id, dag_id, task_id, execution_date))
        conn.commit()

    logger.info(f"Pipeline run started: {run_id} ({dag_id})")
    return run_id


def log_pipeline_success(run_id: str, records_loaded: int = 0,
                         metadata: dict = None):
    """Log successful completion of a pipeline run."""
    ensure_pipeline_runs_table_exists()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE raw.pipeline_runs
                SET status = 'success',
                    finished_at = NOW(),
                    records_loaded = %s,
                    metadata = %s
                WHERE run_id = %s
            """, (records_loaded, Json(metadata) if metadata else None, run_id))
        conn.commit()

    logger.info(f"Pipeline run succeeded: {run_id} ({records_loaded} records)")


def log_pipeline_failure(run_id: str, error_message: str,
                         metadata: dict = None):
    """Log failed pipeline run."""
    ensure_pipeline_runs_table_exists()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE raw.pipeline_runs
                SET status = 'failed',
                    finished_at = NOW(),
                    error_message = %s,
                    metadata = %s
                WHERE run_id = %s
            """, (error_message, Json(metadata) if metadata else None, run_id))
        conn.commit()

    logger.error(f"Pipeline run failed: {run_id} — {error_message}")
