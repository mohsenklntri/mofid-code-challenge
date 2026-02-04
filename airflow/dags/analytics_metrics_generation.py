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
    dag_id="analytics_metrics_generation",
    description="Generate business metrics for management dashboards",
    default_args=default_args,
    start_date=datetime(2020, 1, 30),
    schedule_interval="@monthly",  # Run monthly
    catchup=True,
    max_active_runs=1,
    tags=["analytics", "metrics", "postgres"],
) as dag:

    @task
    def create_analytics_schema():
        """Create analytics schema and metric tables"""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with get_db_connection(hook) as conn:
            cursor = conn.cursor()

            # Test connection
            cursor.execute("SELECT 1")
            logging.info("Database connection successful")

            # Create analytics schema
            cursor.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            logging.info("Schema 'analytics' created/verified")

            # ========================================
            # TABLE 1: Daily National Metrics
            # ========================================
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics.daily_national_metrics (
                    metric_date DATE PRIMARY KEY,
                    
                    -- Daily counts
                    new_cases_total INT,
                    new_deaths_total INT,
                    
                    -- Cumulative counts
                    cumulative_cases BIGINT,
                    cumulative_deaths BIGINT,
                    
                    -- Averages
                    avg_case_fatality_rate NUMERIC(5,2),
                    
                    -- Trends (7-day moving averages)
                    cases_7day_avg NUMERIC(10,2),
                    deaths_7day_avg NUMERIC(10,2),
                    
                    -- Growth rates
                    cases_growth_rate NUMERIC(10,2),  -- % change from previous day
                    deaths_growth_rate NUMERIC(10,2),
                    
                    -- Data quality
                    counties_reporting INT,
                    states_reporting INT,
                    data_quality_score NUMERIC(3,2),  -- 0-1 scale
                    
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """
            )

            # ========================================
            # TABLE 2: State-Level Metrics
            # ========================================
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics.state_metrics (
                    metric_date DATE NOT NULL,
                    state_name TEXT NOT NULL,
                    
                    -- Daily counts
                    new_cases INT,
                    new_deaths INT,
                    
                    -- Cumulative
                    cumulative_cases BIGINT,
                    cumulative_deaths BIGINT,
                    
                    -- Rates
                    case_fatality_rate NUMERIC(5,2),
                    
                    -- Trends
                    cases_7day_avg NUMERIC(10,2),
                    deaths_7day_avg NUMERIC(10,2),
                    
                    -- Rankings
                    cases_rank INT,  -- Rank by new cases
                    deaths_rank INT,
                    
                    -- Counties
                    counties_count INT,
                    
                    created_at TIMESTAMP DEFAULT NOW(),
                    
                    PRIMARY KEY (metric_date, state_name)
                )
            """
            )

            # ========================================
            # TABLE 3: County-Level Metrics (Top performers only)
            # ========================================
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics.county_metrics (
                    metric_date DATE NOT NULL,
                    state_name TEXT NOT NULL,
                    county_name TEXT NOT NULL,
                    county_fips TEXT NOT NULL,
                    
                    -- Metrics
                    new_cases INT,
                    new_deaths INT,
                    cumulative_cases BIGINT,
                    cumulative_deaths BIGINT,
                    case_fatality_rate NUMERIC(5,2),
                    
                    -- Trends
                    cases_7day_avg NUMERIC(10,2),
                    trend_direction TEXT,  -- 'increasing', 'decreasing', 'stable'
                    
                    created_at TIMESTAMP DEFAULT NOW(),
                    
                    PRIMARY KEY (metric_date, county_fips)
                )
            """
            )

            # Create indexes
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_state_metrics_date ON analytics.state_metrics(metric_date)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_state_metrics_state ON analytics.state_metrics(state_name)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_county_metrics_date ON analytics.county_metrics(metric_date)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_county_metrics_county ON analytics.county_metrics(county_name)"
            )

            logging.info("Analytics tables and indexes created/verified")

            conn.commit()

    @task
    def generate_daily_national_metrics(execution_date):
        """Generate daily national-level metrics"""

        start_date, end_date = get_month_start_end(execution_date)
        logging.info(f"Processing range {start_date} -> {end_date}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with get_db_connection(hook) as conn:
            cursor = conn.cursor()

            cursor.execute(
                f"""
                INSERT INTO analytics.daily_national_metrics (
                    metric_date,
                    new_cases_total,
                    new_deaths_total,
                    cumulative_cases,
                    cumulative_deaths,
                    avg_case_fatality_rate,
                    cases_7day_avg,
                    deaths_7day_avg,
                    cases_growth_rate,
                    deaths_growth_rate,
                    counties_reporting,
                    states_reporting,
                    data_quality_score
                )
                SELECT 
                    report_date as metric_date,
                    SUM(positive_new_cases) as new_cases_total,
                    SUM(death_new_count) as new_deaths_total,
                    SUM(positive_total) as cumulative_cases,
                    SUM(death_total) as cumulative_deaths,
                    ROUND(AVG(case_fatality_rate), 2) as avg_case_fatality_rate,
                    
                    -- 7-day moving average
                    ROUND(AVG(SUM(positive_new_cases)) OVER (
                        ORDER BY report_date 
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ), 2) as cases_7day_avg,
                    
                    ROUND(AVG(SUM(death_new_count)) OVER (
                        ORDER BY report_date 
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ), 2) as deaths_7day_avg,
                    
                    -- Growth rate (compared to yesterday)
                    CASE 
                        WHEN LAG(SUM(positive_new_cases)) OVER (ORDER BY report_date) > 0 THEN
                            ROUND(
                                ((SUM(positive_new_cases) - LAG(SUM(positive_new_cases)) OVER (ORDER BY report_date))::NUMERIC 
                                / LAG(SUM(positive_new_cases)) OVER (ORDER BY report_date) * 100), 
                                2
                            )
                        ELSE 0
                    END as cases_growth_rate,
                    
                    CASE 
                        WHEN LAG(SUM(death_new_count)) OVER (ORDER BY report_date) > 0 THEN
                            ROUND(
                                ((SUM(death_new_count) - LAG(SUM(death_new_count)) OVER (ORDER BY report_date))::NUMERIC 
                                / LAG(SUM(death_new_count)) OVER (ORDER BY report_date) * 100), 
                                2
                            )
                        ELSE 0
                    END as deaths_growth_rate,
                    
                    COUNT(DISTINCT county_fips) as counties_reporting,
                    COUNT(DISTINCT state_name) as states_reporting,
                    
                    -- Data quality score (1 - anomaly rate)
                    ROUND(1.0 - (SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)::NUMERIC / COUNT(*)), 2) as data_quality_score
                    
                FROM staging.covid_data
                WHERE report_date >= '{start_date}'
                    AND report_date < '{end_date}'
                GROUP BY report_date
                ORDER BY report_date
                
                ON CONFLICT (metric_date)
                DO UPDATE SET
                    new_cases_total = EXCLUDED.new_cases_total,
                    new_deaths_total = EXCLUDED.new_deaths_total,
                    cumulative_cases = EXCLUDED.cumulative_cases,
                    cumulative_deaths = EXCLUDED.cumulative_deaths,
                    avg_case_fatality_rate = EXCLUDED.avg_case_fatality_rate,
                    cases_7day_avg = EXCLUDED.cases_7day_avg,
                    deaths_7day_avg = EXCLUDED.deaths_7day_avg,
                    cases_growth_rate = EXCLUDED.cases_growth_rate,
                    deaths_growth_rate = EXCLUDED.deaths_growth_rate,
                    counties_reporting = EXCLUDED.counties_reporting,
                    states_reporting = EXCLUDED.states_reporting,
                    data_quality_score = EXCLUDED.data_quality_score,
                    created_at = NOW()
            """
            )

            logging.info(
                f"Generated daily national metrics form {start_date} to {end_date}"
            )
            conn.commit()

    @task
    def generate_state_metrics(execution_date):
        """Generate state-level metrics"""

        start_date, end_date = get_month_start_end(execution_date)
        logging.info(f"Processing range {start_date} -> {end_date}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with get_db_connection(hook) as conn:
            cursor = conn.cursor()

            cursor.execute(
                f"""
                WITH state_daily AS (
                    SELECT 
                        report_date,
                        state_name,
                        SUM(positive_new_cases) as new_cases,
                        SUM(death_new_count) as new_deaths,
                        SUM(positive_total) as cumulative_cases,
                        SUM(death_total) as cumulative_deaths,
                        ROUND(AVG(case_fatality_rate), 2) as case_fatality_rate,
                        COUNT(DISTINCT county_fips) as counties_count,
                        
                        -- 7-day moving average per state
                        ROUND(AVG(SUM(positive_new_cases)) OVER (
                            PARTITION BY state_name 
                            ORDER BY report_date 
                            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                        ), 2) as cases_7day_avg,
                        
                        ROUND(AVG(SUM(death_new_count)) OVER (
                            PARTITION BY state_name 
                            ORDER BY report_date 
                            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                        ), 2) as deaths_7day_avg
                        
                    FROM staging.covid_data
                    WHERE report_date >= '{start_date}'
                        AND report_date < '{end_date}'
                    GROUP BY report_date, state_name
                ),
                ranked AS (
                    SELECT 
                        *,
                        RANK() OVER (PARTITION BY report_date ORDER BY new_cases DESC) as cases_rank,
                        RANK() OVER (PARTITION BY report_date ORDER BY new_deaths DESC) as deaths_rank
                    FROM state_daily
                )
                INSERT INTO analytics.state_metrics
                SELECT 
                    report_date as metric_date,
                    state_name,
                    new_cases,
                    new_deaths,
                    cumulative_cases,
                    cumulative_deaths,
                    case_fatality_rate,
                    cases_7day_avg,
                    deaths_7day_avg,
                    cases_rank,
                    deaths_rank,
                    counties_count,
                    NOW() as created_at
                FROM ranked
                
                ON CONFLICT (metric_date, state_name)
                DO UPDATE SET
                    new_cases = EXCLUDED.new_cases,
                    new_deaths = EXCLUDED.new_deaths,
                    cumulative_cases = EXCLUDED.cumulative_cases,
                    cumulative_deaths = EXCLUDED.cumulative_deaths,
                    case_fatality_rate = EXCLUDED.case_fatality_rate,
                    cases_7day_avg = EXCLUDED.cases_7day_avg,
                    deaths_7day_avg = EXCLUDED.deaths_7day_avg,
                    cases_rank = EXCLUDED.cases_rank,
                    deaths_rank = EXCLUDED.deaths_rank,
                    counties_count = EXCLUDED.counties_count,
                    created_at = NOW()
            """
            )

            logging.info(
                f"Generated state-level metrics from {start_date} to {end_date}"
            )
            conn.commit()

    @task
    def generate_county_metrics(execution_date):
        """Generate county-level metrics (top 100 counties only)"""

        start_date, end_date = get_month_start_end(execution_date)
        logging.info(f"Processing range {start_date} -> {end_date}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with get_db_connection(hook) as conn:
            cursor = conn.cursor()

            cursor.execute(
                f"""
                WITH county_daily AS (
                    SELECT 
                        report_date,
                        state_name,
                        county_name,
                        county_fips,
                        positive_new_cases as new_cases,
                        death_new_count as new_deaths,
                        positive_total as cumulative_cases,
                        death_total as cumulative_deaths,
                        case_fatality_rate,
                        
                        -- 7-day moving average per county
                        ROUND(AVG(positive_new_cases) OVER (
                            PARTITION BY county_fips 
                            ORDER BY report_date 
                            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                        ), 2) as cases_7day_avg,
                        
                        -- Trend direction
                        CASE 
                            WHEN AVG(positive_new_cases) OVER (
                                PARTITION BY county_fips 
                                ORDER BY report_date 
                                ROWS BETWEEN 6 PRECEDING AND 3 PRECEDING
                            ) < AVG(positive_new_cases) OVER (
                                PARTITION BY county_fips 
                                ORDER BY report_date 
                                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                            ) THEN 'increasing'
                            WHEN AVG(positive_new_cases) OVER (
                                PARTITION BY county_fips 
                                ORDER BY report_date 
                                ROWS BETWEEN 6 PRECEDING AND 3 PRECEDING
                            ) > AVG(positive_new_cases) OVER (
                                PARTITION BY county_fips 
                                ORDER BY report_date 
                                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                            ) THEN 'decreasing'
                            ELSE 'stable'
                        END as trend_direction
                        
                    FROM staging.covid_data
                    WHERE report_date >= '{start_date}'
                        AND report_date < '{end_date}'
                ),
                top_counties AS (
                    SELECT 
                        *,
                        RANK() OVER (PARTITION BY report_date ORDER BY new_cases DESC) as rank
                    FROM county_daily
                )
                INSERT INTO analytics.county_metrics
                SELECT 
                    report_date as metric_date,
                    state_name,
                    county_name,
                    county_fips,
                    new_cases,
                    new_deaths,
                    cumulative_cases,
                    cumulative_deaths,
                    case_fatality_rate,
                    cases_7day_avg,
                    trend_direction,
                    NOW() as created_at
                FROM top_counties
                WHERE rank <= 100  -- Only top 100 counties per day
                
                ON CONFLICT (metric_date, county_fips)
                DO UPDATE SET
                    state_name = EXCLUDED.state_name,
                    county_name = EXCLUDED.county_name,
                    new_cases = EXCLUDED.new_cases,
                    new_deaths = EXCLUDED.new_deaths,
                    cumulative_cases = EXCLUDED.cumulative_cases,
                    cumulative_deaths = EXCLUDED.cumulative_deaths,
                    case_fatality_rate = EXCLUDED.case_fatality_rate,
                    cases_7day_avg = EXCLUDED.cases_7day_avg,
                    trend_direction = EXCLUDED.trend_direction,
                    created_at = NOW()
            """
            )

            logging.info(
                f"Generated county-level metrics for top 100 counties for date range {start_date} to {end_date}"
            )
            conn.commit()

    # Task dependencies
    schema = create_analytics_schema()
    daily = generate_daily_national_metrics(execution_date="{{ ds }}")
    states = generate_state_metrics(execution_date="{{ ds }}")
    counties = generate_county_metrics(execution_date="{{ ds }}")

    schema >> [daily, states, counties]
