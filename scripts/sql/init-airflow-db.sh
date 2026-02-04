#!/usr/bin/env bash

echo "Creating airflow database..." >&2
psql -U "${POSTGRES_USER}" -d postgres <<-EOSQL
    SELECT 'CREATE DATABASE "${AIRFLOW_POSTGRES_DB}"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${AIRFLOW_POSTGRES_DB}')\gexec

    GRANT ALL PRIVILEGES ON DATABASE "${AIRFLOW_POSTGRES_DB}" TO "${POSTGRES_USER}";
EOSQL

if [ $? -eq 0 ]; then
    echo "Airflow database '${AIRFLOW_POSTGRES_DB}' is ready." >&2
else
    echo "Failed to create airflow database." >&2
    exit 1
fi
