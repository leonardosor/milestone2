#!/usr/bin/env bash
set -euo pipefail

echo "[entrypoint] Starting container at $(date)"

# Default values
: "${APP_CONFIG:=config.container.json}"
: "${DB_HOST:=db}"
: "${DB_PORT:=5432}"
: "${DB_NAME:=milestone2}"
: "${DB_USER:=postgres}"
: "${DB_PASSWORD:=postgres}"
: "${WAIT_FOR_DB:=true}"
: "${RUN_DBT:=false}"
: "${DBT_TARGET:=dev}"
: "${DBT_PROFILES_DIR:=/app/dbt_project}"
: "${MAIN_MODULE:=src/main.py}"
: "${ENABLE_QA:=false}"

if [ ! -f "$APP_CONFIG" ]; then
  echo "[entrypoint] Copying default container config"
  cp config.container.json "$APP_CONFIG" || true
fi

if [ "$WAIT_FOR_DB" = "true" ]; then
  echo "[entrypoint] Waiting for Postgres $DB_HOST:$DB_PORT ..."
  for i in {1..60}; do
    if PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c 'SELECT 1;' >/dev/null 2>&1; then
      echo "[entrypoint] Database is ready"; break
    fi
    echo "[entrypoint] Retry $i/60 ..."; sleep 2
    if [ "$i" -eq 60 ]; then
      echo "[entrypoint] ERROR: Database not reachable"; exit 1
    fi
  done
fi

# Enable PostGIS extension (idempotent)
PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS postgis;" || echo "[entrypoint] Could not create PostGIS extension (ignore if exists)"

# Export variables for dbt profile
export LOCAL_HOST="$DB_HOST"
export LOCAL_PORT="$DB_PORT"
export LOCAL_DB="$DB_NAME"
export LOCAL_USER="$DB_USER"
export LOCAL_PW="$DB_PASSWORD"

# QA flags
if [ "$ENABLE_QA" = "true" ]; then
  export QA_MODE=true
fi

cmd="${1:-run-etl}"
shift || true

case "$cmd" in
  run-etl)
    echo "[entrypoint] Running orchestrated ETL"
    python "$MAIN_MODULE" --config "$APP_CONFIG" "$@"
    ;;
  census)
    python src/census_data.py --begin-year 2014 --end-year 2014 --config "$APP_CONFIG" "$@"
    ;;
  urban)
    python src/urban_data.py --config "$APP_CONFIG" "$@"
    ;;
  location)
    python src/location_data.py "$@"
    ;;
  dbt-run)
    if [ "$RUN_DBT" = "true" ]; then
      echo "[entrypoint] Executing dbt run"
      dbt run --profiles-dir "$DBT_PROFILES_DIR" --target "$DBT_TARGET"
    else
      echo "[entrypoint] RUN_DBT not enabled"
    fi
    ;;
  dbt-test)
    dbt test --profiles-dir "$DBT_PROFILES_DIR" --target "$DBT_TARGET"
    ;;
  bash)
    exec bash "$@"
    ;;
  *)
    echo "[entrypoint] Executing custom command: $cmd $*"
    exec "$cmd" "$@"
    ;;
 esac
