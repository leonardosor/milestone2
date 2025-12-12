#!/usr/bin/env bash
set -euo pipefail

echo "[entrypoint] Starting ETL container at $(date)"

# Environment defaults
: "${DB_HOST:=db}"
: "${DB_PORT:=5432}"
: "${DB_NAME:=milestone2}"
: "${DB_USER:=postgres}"
: "${DB_PASSWORD:=postgres}"
: "${DB_SCHEMA:=test}"
: "${DATABASE_TYPE:=local}"
: "${WAIT_FOR_DB:=true}"
: "${APP_CONFIG:=/app/config/config.json}"
: "${DBT_TARGET:=dev}"
: "${DBT_PROFILES_DIR:=/app/dbt_project}"
: "${ENABLE_QA:=false}"

# Generate config.json from environment if not exists
if [ ! -f "$APP_CONFIG" ]; then
    echo "[entrypoint] Generating config.json from environment..."
    mkdir -p "$(dirname "$APP_CONFIG")"
    cat > "$APP_CONFIG" <<EOF
{
  "database_type": "$DATABASE_TYPE",
  "local_database": {
    "host": "$DB_HOST",
    "port": $DB_PORT,
    "database": "$DB_NAME",
    "username": "$DB_USER",
    "password": "$DB_PASSWORD"
  },
  "env_database": {
    "host": "$DB_HOST",
    "port": $DB_PORT,
    "database": "$DB_NAME",
    "username": "$DB_USER",
    "password": "$DB_PASSWORD"
  },
  "schema": "$DB_SCHEMA",
  "etl": {
    "census_years": [2014, 2024],
    "urban_years": [2014, 2024],
    "batch_size": 1000
  },
  "async": {
    "max_concurrent_requests": 10,
    "db_batch_size": 1000,
    "connection_pool_size": 10,
    "max_overflow": 20
  },
  "census": {
    "rate_limit_delay": 1
  },
  "urban": {
    "base_url": "https://educationdata.urban.org"
  }
}
EOF
    echo "[entrypoint] Config file generated at $APP_CONFIG"
fi

# Wait for database
if [ "$WAIT_FOR_DB" = "true" ]; then
    echo "[entrypoint] Waiting for PostgreSQL at $DB_HOST:$DB_PORT..."
    for i in {1..60}; do
        if PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c 'SELECT 1;' >/dev/null 2>&1; then
            echo "[entrypoint] Database ready"
            break
        fi
        echo "[entrypoint] Retry $i/60..."
        sleep 2
        if [ "$i" -eq 60 ]; then
            echo "[entrypoint] ERROR: Database not reachable after 60 attempts"
            exit 1
        fi
    done
fi

# Enable PostGIS extension (idempotent)
echo "[entrypoint] Ensuring PostGIS extension..."
PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS postgis;" 2>/dev/null || echo "[entrypoint] PostGIS extension already exists or cannot be created"

# Export environment variables for dbt and other tools
export LOCAL_HOST="$DB_HOST"
export LOCAL_PORT="$DB_PORT"
export LOCAL_DB="$DB_NAME"
export LOCAL_USER="$DB_USER"
export LOCAL_PW="$DB_PASSWORD"

# QA flags
if [ "$ENABLE_QA" = "true" ]; then
    export QA_MODE=true
    export QA_BREAKPOINTS=true
fi

# Command routing
cmd="${1:-run-etl}"
shift || true

case "$cmd" in
    run-etl)
        echo "[entrypoint] Running orchestrated ETL pipeline"
        python /app/src/main.py --config "$APP_CONFIG" "$@"
        ;;
    census)
        echo "[entrypoint] Running Census ETL"
        python /app/src/census_data.py --config "$APP_CONFIG" "$@"
        ;;
    urban)
        echo "[entrypoint] Running Urban Institute ETL"
        python /app/src/urban_data.py --config "$APP_CONFIG" "$@"
        ;;
    location)
        echo "[entrypoint] Running Location Data ETL"
        python /app/src/location_data.py "$@"
        ;;
    dbt-run)
        echo "[entrypoint] Executing dbt run"
        cd /app/dbt_project
        dbt run --profiles-dir . --target "$DBT_TARGET"
        ;;
    dbt-test)
        echo "[entrypoint] Executing dbt test"
        cd /app/dbt_project
        dbt test --profiles-dir . --target "$DBT_TARGET"
        ;;
    dbt-docs)
        echo "[entrypoint] Generating dbt docs"
        cd /app/dbt_project
        dbt docs generate --profiles-dir . --target "$DBT_TARGET"
        ;;
    explorer)
        echo "[entrypoint] Running database explorer"
        python /app/src/database_explorer.py
        ;;
    test-config)
        echo "[entrypoint] Testing configuration loading"
        python /app/config/config_loader.py
        ;;
    bash)
        echo "[entrypoint] Starting bash shell"
        exec bash "$@"
        ;;
    *)
        echo "[entrypoint] Executing custom command: $cmd $*"
        exec "$cmd" "$@"
        ;;
esac

echo "[entrypoint] Command completed at $(date)"
