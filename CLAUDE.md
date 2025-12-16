# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a containerized data analytics platform that extracts, transforms, and analyzes census and education data. The system consists of three main services:
1. **PostgreSQL database** (with PostGIS extension)
2. **Streamlit web interface** for ETL control, database exploration, and ML dashboards
3. **ETL container** for data ingestion from US Census Bureau and Urban Institute APIs

## Architecture

### Microservices Structure
- **milestone2-db**: PostgreSQL 15 with PostGIS extension (port 5432)
- **milestone2-streamlit**: Streamlit web app (port 8501)
- **milestone2-etl**: ETL processing service (on-demand execution)

### Data Flow
1. ETL extracts data from Census API and Urban Institute API
2. Raw data lands in PostgreSQL (typically `test` schema)
3. dbt transforms raw data into staging and mart models
4. Streamlit provides interactive access to all layers

### Configuration Management
- **Environment-first**: Services read from `.env` file (see `.env.example`)
- **Config fallback**: `config.json` provides detailed ETL configuration when environment variables aren't sufficient
- **Dual-mode database**: Supports both local PostgreSQL and Supabase (controlled by `DATABASE_TYPE` env var)
- **ConfigLoader**: The `etl/config/config_loader.py` merges environment variables with JSON config

## Common Commands

### Docker Operations
```bash
# Start all services
docker-compose -f docker-compose.prod.yml up --build

# Start only database
docker-compose -f docker-compose.prod.yml up -d db

# Stop all services
docker-compose -f docker-compose.prod.yml down

# View logs
docker-compose -f docker-compose.prod.yml logs [service-name]

# Access container shell
docker exec -it milestone2-etl bash
```

### ETL Execution

**Via Streamlit UI** (recommended):
- Navigate to http://localhost:8501
- Use "ETL Control" page

**Via Docker exec**:
```bash
# Complete pipeline
docker exec milestone2-etl python /app/src/main.py

# Census data only (specific years)
docker exec milestone2-etl python /app/src/main.py --census-only --census-begin-year 2020 --census-end-year 2023

# Urban Institute data only
docker exec milestone2-etl python /app/src/main.py --urban-only

# Location data (geocoding)
docker exec milestone2-etl python /app/src/main.py --location-only

# Using entrypoint shortcuts
docker exec milestone2-etl ./entrypoint.sh census
docker exec milestone2-etl ./entrypoint.sh urban
docker exec milestone2-etl ./entrypoint.sh location
```

### dbt Operations
```bash
# Run all models
docker exec milestone2-etl /bin/bash -c "cd /app/dbt_project && dbt run --profiles-dir ."

# Run specific model
docker exec milestone2-etl /bin/bash -c "cd /app/dbt_project && dbt run --models stg_census_data --profiles-dir ."

# Run tests
docker exec milestone2-etl /bin/bash -c "cd /app/dbt_project && dbt test --profiles-dir ."

# Generate documentation
docker exec milestone2-etl /bin/bash -c "cd /app/dbt_project && dbt docs generate --profiles-dir ."

# Using entrypoint shortcuts
docker exec milestone2-etl ./entrypoint.sh dbt-run
docker exec milestone2-etl ./entrypoint.sh dbt-test
```

### Database Access
```bash
# Connect to database
docker exec -it milestone2-db psql -U postgres -d milestone2

# Run SQL from host
docker exec milestone2-db psql -U postgres -d milestone2 -c "SELECT COUNT(*) FROM test.census_data;"

# Check database readiness
docker exec milestone2-db pg_isready -U postgres
```

### Database Migration
```powershell
# Migrate from local PostgreSQL to containerized (PowerShell)
.\migrate_database.ps1

# With custom parameters
.\migrate_database.ps1 -SourceHost localhost -SourceDB milestone2 -TargetContainer milestone2-db
```

## Key Implementation Details

### ETL Components
- **etl/src/main.py**: Orchestrated ETL controller with CLI arguments
- **etl/src/census_data.py**: Census Bureau API client (SimpleCensusETL class)
- **etl/src/urban_data.py**: Urban Institute API client (EndpointETL class, async)
- **etl/src/location_data.py**: TIGER/Line geocoding (offline processing)

### Configuration Loading Pattern
All ETL scripts use `ConfigLoader` from `etl/config/config_loader.py`:
```python
from config_loader import ConfigLoader
config_loader = ConfigLoader("config.json")
config = config_loader.config
```

This loads environment variables first, then merges with JSON config. Environment variables always take precedence.

### Database Connection Pattern
Streamlit components use `DatabaseConnector` from `app/components/db_connector.py`:
```python
from components.db_connector import DatabaseConnector
db = DatabaseConnector()  # Reads from env vars
df = db.execute_query("SELECT * FROM test.census_data LIMIT 10")
```

### Schema Organization
- **test** schema: Raw ETL landing tables (census_data, urban_data_directory, urban_data_expanded, location_data)
- **public** schema: dbt transformed models
- **dev** schema: dbt development models (when DBT_TARGET=dev)

### dbt Model Structure
```
models/
├── sources.yml           # Defines source tables in 'test' schema
├── staging/              # Cleaned raw data (stg_*)
│   ├── stg_census_data.sql
│   ├── stg_school_directory.sql
│   └── stg_school_assessments.sql
└── marts/                # Business logic models (dim_*)
    └── dim_school_assessments.sql
```

### Async ETL Pattern
The Urban Institute ETL (`urban_data.py`) uses asyncio for concurrent API requests:
```python
await self.urban_etl.ingest(begin_year=2020, end_year=2023)
```

Main orchestrator (`main.py`) uses `asyncio.run(main())` at entry point.

### Entrypoint Command Routing
The `etl/entrypoint.sh` provides shortcuts for common operations:
- `run-etl`: Full pipeline via main.py
- `census`, `urban`, `location`: Individual ETL modules
- `dbt-run`, `dbt-test`, `dbt-docs`: dbt operations
- `test-config`: Validate configuration loading

## Development Workflow

### Modifying ETL Logic
1. Edit files in `etl/src/` directory
2. Volumes are mounted, so changes are reflected immediately
3. Re-run ETL command to test changes
4. No rebuild needed for Python code changes

### Modifying Streamlit Pages
1. Edit files in `app/` directory
2. Streamlit auto-reloads on file changes
3. Refresh browser to see updates
4. No rebuild needed

### Adding New dbt Models
1. Create `.sql` file in `dbt_project/models/staging/` or `dbt_project/models/marts/`
2. Update `schema.yml` with model documentation
3. Run `docker exec milestone2-etl ./entrypoint.sh dbt-run`
4. Check model output in database

### Configuration Changes
When changing database credentials or connection settings:
1. Update `.env` file
2. Restart services: `docker-compose -f docker-compose.prod.yml restart`
3. Verify with: `docker exec milestone2-etl ./entrypoint.sh test-config`

## Data Volumes

Persistent data is stored in Docker volumes:
- `postgres_data`: Database files (critical - contains all data)
- `etl_logs`: ETL execution logs at `/app/logs/`
- `tiger_data`: Downloaded TIGER/Line shapefiles (cacheable)
- `model_artifacts`: ML model outputs from notebooks (optional)

Backup database volume:
```bash
docker run --rm -v milestone2_postgres_data:/data -v $(pwd):/backup ubuntu tar czf /backup/postgres_backup.tar.gz /data
```

## Troubleshooting

### Database Connection Issues
- Verify container is running: `docker ps | grep milestone2-db`
- Check logs: `docker logs milestone2-db`
- Test connection: `docker exec milestone2-db pg_isready -U postgres`
- Verify env vars in Streamlit: Check "System Status" on homepage

### ETL Failures
- View logs: `docker exec milestone2-etl tail -f /app/logs/main_etl.log`
- Enable QA mode for debugging: Set `QA_MODE=true` and `QA_BREAKPOINTS=true` in `.env`
- Test individual components: Use `--census-only`, `--urban-only`, or `--location-only` flags

### dbt Errors
- Check profiles: `docker exec milestone2-etl /bin/bash -c "cd /app/dbt_project && dbt debug --profiles-dir ."`
- Verify source tables exist in `test` schema
- Check schema permissions: dbt needs CREATE privileges on target schema

### Schema Confusion
The project uses multiple schemas:
- Raw data → `test` schema (or override with `DB_SCHEMA` env var)
- dbt output → `public` or `dev` schema (controlled by `DBT_TARGET`)
- Always check which schema you're querying in SQL

If tables are missing in `public` schema, check `test` schema first or run dbt transformations.
