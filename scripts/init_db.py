"""
init_db.py — Initialize database by running all SQL scripts in sql/ directory.
"""

import os
import glob
import logging
import psycopg2.errors
from scripts.db_utils import get_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def split_sql_statements(sql_content: str) -> list:
    """Split SQL content into individual statements.
    
    Handles:
    - Multi-line statements
    - Comments (-- and /* */)
    - PostgreSQL dollar-quoted strings ($$...$$)
    """
    statements = []
    current_statement = []
    in_dollar_quote = False
    dollar_quote_tag = None
    i = 0
    
    lines = sql_content.split('\n')
    for line_num, line in enumerate(lines):
        # Handle comments only if not in a dollar-quoted string
        if not in_dollar_quote and '--' in line:
            line = line[:line.index('--')]
        
        # Track dollar-quoted strings
        j = 0
        processed_line = []
        
        while j < len(line):
            # Check for dollar quote start/end
            if line[j:j+1] == '$':
                # Find the complete dollar quote tag (e.g., $$ or $tag$)
                k = j + 1
                while k < len(line) and (line[k].isalnum() or line[k] == '_'):
                    k += 1
                if k < len(line) and line[k] == '$':
                    tag = line[j:k+1]
                    if in_dollar_quote and tag == dollar_quote_tag:
                        in_dollar_quote = False
                        dollar_quote_tag = None
                        processed_line.append(tag)
                        j = k + 1
                    elif not in_dollar_quote:
                        in_dollar_quote = True
                        dollar_quote_tag = tag
                        processed_line.append(tag)
                        j = k + 1
                    else:
                        processed_line.append(line[j])
                        j += 1
                else:
                    processed_line.append(line[j])
                    j += 1
            else:
                processed_line.append(line[j])
                j += 1
        
        line = ''.join(processed_line)
        line = line.rstrip()
        
        # Skip empty lines
        if not line.strip():
            if in_dollar_quote and current_statement:
                # Keep empty lines inside dollar-quoted strings
                current_statement.append('')
            continue
        
        current_statement.append(line)
        
        # Check if statement ends with semicolon (only if not in dollar quote)
        if not in_dollar_quote and line.endswith(';'):
            statement = '\n'.join(current_statement)
            statement = statement.rstrip()
            if statement.endswith(';'):
                statement = statement[:-1]
            if statement.strip():
                statements.append(statement)
            current_statement = []
    
    # Add any remaining statement
    if current_statement:
        statement = '\n'.join(current_statement)
        statement = statement.rstrip()
        if statement.endswith(';'):
            statement = statement[:-1]
        if statement.strip():
            statements.append(statement)
    
    return statements

def initialize_database():
    """Reads and executes all .sql files in the sql/ directory in order.
    
    Skips data aggregation scripts (005_feature_aggregation.sql) which should
    only run after raw data is loaded.
    """
    
    # Path to SQL files
    sql_dir = os.path.join(os.getcwd(), "sql")
    sql_files = sorted(glob.glob(os.path.join(sql_dir, "*.sql")))
    
    if not sql_files:
        logger.warning(f"No SQL files found in {sql_dir}")
        return

    # Skip data aggregation scripts that require data to be loaded first
    skip_files = {"005_feature_aggregation.sql"}
    sql_files = [f for f in sql_files if os.path.basename(f) not in skip_files]

    # Set connection values for local execution if not present
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("POSTGRES_PORT", "5433")

    logger.info(f"Connecting to database to run {len(sql_files)} SQL scripts...")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Drop and recreate all schemas to ensure clean state
                logger.info("Cleaning up database schemas...")
                cur.execute("DROP SCHEMA IF EXISTS marts CASCADE;")
                cur.execute("DROP SCHEMA IF EXISTS staging CASCADE;")
                cur.execute("DROP SCHEMA IF EXISTS raw CASCADE;")
                
                for sql_file in sql_files:
                    logger.info(f"Executing {os.path.basename(sql_file)}...")
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                        # Split into individual statements and execute each
                        statements = split_sql_statements(sql_content)
                        for statement in statements:
                            if statement.strip():
                                cur.execute(statement)
                
                # Add unique constraint on gtfs_stops.stop_id for data loading
                logger.info("Adding unique constraint on raw.gtfs_stops.stop_id...")
                try:
                    cur.execute("ALTER TABLE raw.gtfs_stops ADD CONSTRAINT unique_stop_id UNIQUE (stop_id);")
                except psycopg2.errors.DuplicateObject:
                    logger.info("Unique constraint already exists, skipping...")
            conn.commit()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

    logger.info("✅ Database initialization completed successfully!")

if __name__ == "__main__":
    initialize_database()
