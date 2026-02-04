import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from contextlib import contextmanager

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# -------------------------
# CONFIGURATION
# -------------------------

POSTGRES_CONN_ID = "postgres_connection"

default_args = {
    "owner": "Mohsen",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
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

def get_month_start_end(execution_date):
    """Calculate start and end dates for the month of execution_date"""
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
    start_date = execution_date.replace(day=1)
    end_date = start_date + relativedelta(months=1)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


# -------------------------
# DAG DEFINITION
# -------------------------

with DAG(
    dag_id="staging_data_transformation",
    description="Transform raw data to staging layer with cleaning and validation",
    default_args=default_args,
    start_date=datetime(2020, 1, 30),
    schedule_interval="@monthly",  # Run monthly
    catchup=True,
    max_active_runs=1,
    tags=["staging", "transformation", "postgres"],
) as dag:

    @task
    def create_staging_schema():
        """Create staging schema and tables"""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with get_db_connection(hook) as conn:
            cursor = conn.cursor()

            # Test connection
            cursor.execute("SELECT 1")
            logging.info("Database connection successful")

            # Create staging schema
            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
            logging.info("Schema 'staging' created/verified")

            # Create staging table (cleaned version of raw)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS staging.covid_data (
                    id BIGSERIAL,
                    report_date DATE NOT NULL,
                    county_fips VARCHAR(16) NOT NULL,
                    county_name VARCHAR(64) NOT NULL,
                    state_name VARCHAR(32) NOT NULL,
                    
                    -- Cleaned metrics (nulls replaced with 0, negatives handled)
                    positive_new_cases INT NOT NULL DEFAULT 0,
                    positive_total INT NOT NULL DEFAULT 0,
                    death_new_count INT NOT NULL DEFAULT 0,
                    death_total INT NOT NULL DEFAULT 0,
                    
                    -- Calculated fields
                    case_fatality_rate NUMERIC(5,2),  -- (death_total / positive_total * 100)
                    is_anomaly BOOLEAN DEFAULT FALSE,  -- Flag for data quality issues
                    
                    -- Metadata
                    data_source VARCHAR(32),
                    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    raw_ingested_at TIMESTAMP NOT NULL,
                    
                    PRIMARY KEY (report_date, county_fips),
                    
                    -- Constraints
                    CHECK (positive_total >= 0),
                    CHECK (death_total >= 0),
                    CHECK (case_fatality_rate >= 0 AND case_fatality_rate <= 100)
                ) PARTITION BY RANGE (report_date)
            """
            )
            logging.info("Table 'staging.covid_data' created/verified")

            # Create monthly partitions
            start = datetime(2020, 1, 1)
            end = datetime(2027, 1, 1)
            current = start

            while current < end:
                next_month = current + relativedelta(months=1)
                partition_name = f"staging.covid_data_{current.strftime('%Y_%m')}"
                start_date = current.strftime("%Y-%m-%d")
                end_date = next_month.strftime("%Y-%m-%d")

                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {partition_name}
                        PARTITION OF staging.covid_data
                            FOR VALUES FROM ('{start_date}') TO ('{end_date}')
                """
                )
                current = next_month

            logging.info("Monthly partitions created/verified for staging.covid_data")

            # Create indexes for better query performance
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_staging_state 
                    ON staging.covid_data(state_name, report_date)
            """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_staging_county 
                    ON staging.covid_data(county_fips, report_date)
            """
            )

            logging.info("Indexes created/verified")

            conn.commit()

    @task
    def transform_raw_to_staging(execution_date):
        """
        Transform raw data to staging with cleaning and validation
        - Handle nulls
        - Calculate derived metrics
        - Flag anomalies
        """

        start_date, end_date = get_month_start_end(execution_date)
        logging.info(f"Processing range {start_date} -> {end_date}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with get_db_connection(hook) as conn:
            cursor = conn.cursor()

            # Clear staging for the target partiotion range (incremental updates)
            cursor.execute(
                f"""
                DELETE FROM staging.covid_data
                WHERE report_date >= '{start_date}'
                    AND report_date < '{end_date}'
            """
            )
            logging.info(
                f"Cleared staging data for target date range {start_date} to {end_date}"
            )

            # Transform and insert with data quality rules
            cursor.execute(
                f"""
                INSERT INTO staging.covid_data (
                    report_date,
                    county_fips,
                    county_name,
                    state_name,
                    positive_new_cases,
                    positive_total,
                    death_new_count,
                    death_total,
                    case_fatality_rate,
                    is_anomaly,
                    data_source,
                    raw_ingested_at
                )
                SELECT 
                    report_date,
                    county_fips,
                    COALESCE(county_name, 'Unknown') as county_name,
                    COALESCE(state_name, 'Unknown') as state_name,
                    
                    -- Keep negative values for corrections, only handle nulls
                    COALESCE(positive_new_cases, 0) as positive_new_cases,
                    CASE 
                        WHEN positive_total < 0 THEN 0 
                        ELSE COALESCE(positive_total, 0) 
                    END as positive_total,
                           
                    
                    COALESCE(death_new_count, 0) as death_new_count,
                    CASE 
                        WHEN death_total < 0 THEN 0 
                        ELSE COALESCE(death_total, 0) 
                    END as death_total,
                    
                    -- Calculate case fatality rate
                    CASE 
                        WHEN COALESCE(positive_total, 0) > 0 THEN 
                            ROUND((COALESCE(death_total, 0)::NUMERIC / positive_total * 100), 2)
                        ELSE 0
                    END as case_fatality_rate,
                    
                    -- Flag anomalies (unrealistic CFR)
                    CASE 
                        WHEN positive_total < death_total THEN TRUE
                        WHEN positive_total > 0 AND (death_total::NUMERIC / positive_total) > 0.5 THEN TRUE
                        WHEN positive_total < 0 THEN TRUE  -- Negative total is anomaly
                        WHEN death_total < 0 THEN TRUE      -- Negative total is anomaly
                        ELSE FALSE
                    END as is_anomaly,        
                                       
                    data_source,
                    ingested_at as raw_ingested_at
                    
                FROM raw.covid_data
                WHERE report_date >= '{start_date}'
                    AND report_date < '{end_date}'
                    AND report_date IS NOT NULL
                    AND county_fips IS NOT NULL
                
                ON CONFLICT (report_date, county_fips) 
                DO UPDATE SET
                    county_name = EXCLUDED.county_name,
                    state_name = EXCLUDED.state_name,
                    positive_new_cases = EXCLUDED.positive_new_cases,
                    positive_total = EXCLUDED.positive_total,
                    death_new_count = EXCLUDED.death_new_count,
                    death_total = EXCLUDED.death_total,
                    case_fatality_rate = EXCLUDED.case_fatality_rate,
                    is_anomaly = EXCLUDED.is_anomaly,
                    data_source = EXCLUDED.data_source,
                    raw_ingested_at = EXCLUDED.raw_ingested_at,
                    processed_at = NOW()
            """
            )

            rows_affected = cursor.rowcount
            logging.info(f"Transformed {rows_affected} rows from raw to staging")

            if rows_affected == 0:
                logging.warning(
                    "No data transformed for the given date range. Check raw data availability."
                )

                result = {
                    "rows_transformed": 0,
                    "total_rows": 0,
                    "anomaly_count": 0,
                    "avg_cfr": 0.0,
                }

            else:
                # Log data quality metrics
                cursor.execute(
                    f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count,
                        AVG(case_fatality_rate) as avg_cfr,
                        SUM(positive_new_cases) as total_new_cases,
                        SUM(death_new_count) as total_new_deaths
                    FROM staging.covid_data
                    WHERE report_date >= '{start_date}'
                        AND report_date < '{end_date}'
                """
                )

                metrics = cursor.fetchone()
                logging.info(
                    f"Staging metrics: rows={metrics[0]}, anomalies={metrics[1]}, "
                    f"avg_cfr={metrics[2]:.2f}%, new_cases={metrics[3]}, new_deaths={metrics[4]}"
                )

                result = {
                    "rows_transformed": rows_affected,
                    "total_rows": metrics[0] if metrics[0] else 0,
                    "anomaly_count": metrics[1] if metrics[1] else 0,
                    "avg_cfr": float(metrics[2]) if metrics[2] else 0,
                }

            conn.commit()

            return result

    @task
    def validate_staging_data(execution_date):
        """Validate staging data quality"""

        start_date, end_date = get_month_start_end(execution_date)
        logging.info(f"Validating staging data for range {start_date} -> {end_date}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with get_db_connection(hook) as conn:
            cursor = conn.cursor()

            # Check for data consistency
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM staging.covid_data
                WHERE positive_total < death_total
            """
            )
            inconsistent = cursor.fetchone()[0]

            if inconsistent > 0:
                logging.warning(f"Found {inconsistent} rows where deaths > total cases")

            # Check for anomaly rate
            cursor.execute(
                f"""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies
                FROM staging.covid_data
                WHERE report_date >= '{start_date}'
                    AND report_date < '{end_date}'
            """
            )

            total, anomalies = cursor.fetchone()
            if total == 0:
                logging.warning("No data found in staging for the given date range")
                logging.info(f"Anomaly rate: 0.00% (0/0)")
            else:
                anomaly_rate = (anomalies / total * 100) if total > 0 else 0
                logging.info(f"Anomaly rate: {anomaly_rate:.2f}% ({anomalies}/{total})")
                if anomaly_rate > 10:
                    logging.warning(f"High anomaly rate: {anomaly_rate:.2f}%")

            logging.info("✓ Staging validation completed")

    # Task dependencies
    schema = create_staging_schema()
    transform_result = transform_raw_to_staging(execution_date="{{ ds }}")
    validation = validate_staging_data(execution_date="{{ ds }}")

    schema >> transform_result >> validation
