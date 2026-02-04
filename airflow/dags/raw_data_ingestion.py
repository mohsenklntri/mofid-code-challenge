import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
from contextlib import contextmanager

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

# -------------------------
# CONFIGURATION
# -------------------------

CSV_PATH = "/opt/airflow/data/covid-data.csv"
POSTGRES_CONN_ID = "postgres_connection"
CHUNK_SIZE = 100_000
BATCH_SIZE = 5_000

# Quality thresholds
MAX_NULL_PERCENT = 10.0
MAX_NEGATIVE_PERCENT = 5.0

default_args = {
    "owner": "Mohsen",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=15),
}

# -------------------------
# HELPER FUNCTIONS
# -------------------------

@contextmanager
def get_db_connection(hook):
    """Context manager for connection and transaction management"""
    conn = hook.get_conn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Transaction failed: {e}")
        raise
    finally:
        conn.close()


def calculate_quality_metrics(chunk, chunk_idx):
    """Calculate and log data quality metrics"""
    total = len(chunk)
    null_date = chunk["REPORT_DATE"].isna().sum()
    null_fips = chunk["COUNTY_FIPS_NUMBER"].isna().sum()
    negative_cases = (chunk["PEOPLE_POSITIVE_NEW_CASES_COUNT"] < 0).sum()
    negative_deaths = (chunk["PEOPLE_DEATH_NEW_COUNT"] < 0).sum()
    duplicates = chunk.duplicated(subset=["REPORT_DATE", "COUNTY_FIPS_NUMBER"]).sum()
    
    null_pct = (null_date + null_fips) / (total * 2) * 100
    negative_pct = (negative_cases + negative_deaths) / (total * 2) * 100
    
    logging.info(
        f"Chunk {chunk_idx} Quality: "
        f"Nulls={null_date + null_fips} ({null_pct:.2f}%), "
        f"Negatives={negative_cases + negative_deaths} ({negative_pct:.2f}%), "
        f"Duplicates={duplicates}"
    )
    
    # Warning if threshold exceeded
    if null_pct > MAX_NULL_PERCENT:
        logging.warning(f"Chunk {chunk_idx}: High null percentage {null_pct:.2f}%")
    if negative_pct > MAX_NEGATIVE_PERCENT:
        logging.warning(f"Chunk {chunk_idx}: High negative percentage {negative_pct:.2f}%")
    
    return {
        "nulls": null_date + null_fips,
        "negatives": negative_cases + negative_deaths,
        "duplicates": duplicates,
    }


def prepare_chunk(chunk):
    """Prepare chunk: remove duplicates and rename columns"""
    # Remove duplicates (keep last)
    chunk = chunk.drop_duplicates(
        subset=["REPORT_DATE", "COUNTY_FIPS_NUMBER"], 
        keep="last"
    )

    # Filter out rows with null primary keys
    chunk = chunk.dropna(subset=["REPORT_DATE", "COUNTY_FIPS_NUMBER"])
    
    # Rename columns
    chunk = chunk.rename(
        columns={
            "REPORT_DATE": "report_date",
            "COUNTY_FIPS_NUMBER": "county_fips",
            "COUNTY_NAME": "county_name",
            "PROVINCE_STATE_NAME": "state_name",
            "PEOPLE_POSITIVE_NEW_CASES_COUNT": "positive_new_cases",
            "PEOPLE_POSITIVE_CASES_COUNT": "positive_total",
            "PEOPLE_DEATH_NEW_COUNT": "death_new_count",
            "PEOPLE_DEATH_COUNT": "death_total",
            "DATA_SOURCE_NAME": "data_source",
        }
    )
    
    # Select only the columns we need in the exact order
    chunk = chunk[[
        "report_date",
        "county_fips", 
        "county_name",
        "state_name",
        "positive_new_cases",
        "positive_total",
        "death_new_count",
        "death_total",
        "data_source"
    ]]

    # Convert NaT to None
    chunk = chunk.where(pd.notnull(chunk), None)
    
    return chunk


def batch_insert(cursor, records, batch_size=BATCH_SIZE):
    """Insert using execute_values for performance"""
    insert_sql = """
        INSERT INTO raw.covid_data (
            report_date, county_fips, county_name, state_name,
            positive_new_cases, positive_total,
            death_new_count, death_total, data_source
        )
        VALUES %s
        ON CONFLICT (report_date, county_fips)
        DO UPDATE SET
            positive_new_cases = EXCLUDED.positive_new_cases,
            positive_total = EXCLUDED.positive_total,
            death_new_count = EXCLUDED.death_new_count,
            death_total = EXCLUDED.death_total,
            data_source = EXCLUDED.data_source
    """
    
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        execute_values(cursor, insert_sql, batch, page_size=batch_size)
        total += len(batch)
    
    return total


# -------------------------
# DAG DEFINITION
# -------------------------

with DAG(
    dag_id="raw_data_ingestion",
    description="Raw ingestion of COVID CSV data into PostgreSQL",
    default_args=default_args,
    start_date=datetime(2020, 1, 22),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["raw", "ingestion", "postgres"],
) as dag:

    @task
    def validate_prerequisites():
        """Pre-flight checks: file and connection validation, create schema/tables if needed"""
        # Check if file exists
        if not Path(CSV_PATH).exists():
            raise AirflowFailException(f"CSV file not found: {CSV_PATH}")
        
        file_size_mb = Path(CSV_PATH).stat().st_size / (1024 * 1024)
        logging.info(f"CSV file found: {file_size_mb:.2f} MB")
        
        # Check database connection and create schema/tables
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with get_db_connection(hook) as conn:
            cursor = conn.cursor()

            # Test connection
            cursor.execute("SELECT 1")
            logging.info("Database connection successful")
            
            # Create schema
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
            logging.info("Schema 'raw' verified/created")

            # Create raw table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw.covid_data
                (
                    report_date        DATE      NOT NULL,
                    county_fips        TEXT      NOT NULL,
                    county_name        TEXT,
                    state_name         TEXT      NOT NULL,
                    positive_new_cases INT,
                    positive_total     INT CHECK (positive_total >= 0),
                    death_new_count    INT,
                    death_total        INT CHECK (death_total >= 0),

                    data_source        TEXT,
                    ingested_at        TIMESTAMP NOT NULL DEFAULT now(),

                    PRIMARY KEY (report_date, county_fips)
                ) PARTITION BY RANGE (report_date)
            """)
            logging.info("Table 'raw.covid_data' verified/created")

            # Create monthly partitions (2020-01 to 2026-12)
            start = datetime(2020, 1, 1)
            end = datetime(2027, 1, 1)  # Up to end of 2026
            current = start
            
            partition_count = 0
            while current < end:
                next_month = current + relativedelta(months=1)
                
                partition_name = f"raw.covid_data_{current.strftime('%Y_%m')}"
                start_date = current.strftime('%Y-%m-%d')
                end_date = next_month.strftime('%Y-%m-%d')
                
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {partition_name}
                        PARTITION OF raw.covid_data
                            FOR VALUES FROM ('{start_date}') TO ('{end_date}')
                """)
                
                partition_count += 1
                current = next_month
            
            logging.info(f"Created/verified {partition_count} monthly partitions (2020-01 to 2026-12)")
            
            # Create default partition
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw.covid_data_default
                    PARTITION OF raw.covid_data
                        DEFAULT
            """)
            logging.info("Default partition 'raw.covid_data_default' verified/created")
            
            conn.commit()
        
        return {"file_size_mb": file_size_mb}

    @task
    def ingest_csv_to_postgres(pre_check):
        """
        Stream CSV in chunks with quality checks and optimized inserts
        """
        required_columns = {
            "REPORT_DATE",
            "COUNTY_FIPS_NUMBER",
            "COUNTY_NAME",
            "PROVINCE_STATE_NAME",
            "PEOPLE_POSITIVE_NEW_CASES_COUNT",
            "PEOPLE_POSITIVE_CASES_COUNT",
            "PEOPLE_DEATH_NEW_COUNT",
            "PEOPLE_DEATH_COUNT",
            "DATA_SOURCE_NAME",
        }
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        total_rows = 0
        quality_summary = {"nulls": 0, "negatives": 0, "duplicates": 0}
        
        with get_db_connection(hook) as conn:
            cursor = conn.cursor()
            
            for chunk_idx, chunk in enumerate(
                pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, parse_dates=["REPORT_DATE"])
            ):
                logging.info(f"Processing chunk {chunk_idx}...")
                
                # 1. Validate schema
                missing = required_columns - set(chunk.columns)
                if missing:
                    raise AirflowFailException(f"Missing columns: {missing}")
                
                # 2. Calculate quality metrics
                metrics = calculate_quality_metrics(chunk, chunk_idx)
                quality_summary["nulls"] += metrics["nulls"]
                quality_summary["negatives"] += metrics["negatives"]
                quality_summary["duplicates"] += metrics["duplicates"]
                
                # 3. Prepare chunk
                chunk = prepare_chunk(chunk)
                
                # 4. Convert to tuples - use itertuples for proper None conversion
                records = [
                    tuple(None if pd.isna(val) else val for val in row)
                    for row in chunk.to_numpy()
                ]
                
                # 5. Batch insert with execute_values (faster)
                inserted = batch_insert(cursor, records)
                total_rows += inserted
                
                logging.info(f"Chunk {chunk_idx} completed: {inserted} rows inserted")
        
        logging.info(f"Total ingested: {total_rows} rows")
        logging.info(f"Quality summary: {quality_summary}")
        
        return {
            "total_rows": total_rows,
            "quality_summary": quality_summary
        }

    @task
    def post_load_quality_check(result):
        """Post-load quality validation"""
        if result["total_rows"] <= 0:
            raise AirflowFailException("No rows were ingested")
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        with get_db_connection(hook) as conn:
            cursor = conn.cursor()
            
            # Check total count
            cursor.execute("SELECT COUNT(*) FROM raw.covid_data")
            db_count = cursor.fetchone()[0]
            logging.info(f"Total rows in database: {db_count}")
            
            # Check for null keys
            cursor.execute(
                "SELECT COUNT(*) FROM raw.covid_data "
                "WHERE report_date IS NULL OR county_fips IS NULL"
            )
            null_keys = cursor.fetchone()[0]
            if null_keys > 0:
                logging.warning(f"Found {null_keys} rows with NULL primary keys")
            
            # Check date range
            cursor.execute(
                "SELECT MIN(report_date), MAX(report_date) FROM raw.covid_data"
            )
            min_date, max_date = cursor.fetchone()
            logging.info(f"Date range: {min_date} to {max_date}")
        
        logging.info(f"✓ Quality check passed")
        logging.info(f"✓ Summary: {result['quality_summary']}")

    # Task dependencies
    pre_check = validate_prerequisites()
    result = ingest_csv_to_postgres(pre_check)
    post_load_quality_check(result)