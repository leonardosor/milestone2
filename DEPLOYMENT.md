# Milestone 2 - Deployment Guide

## Microservices Architecture - Quick Start

This project has been refactored into a containerized microservices architecture with a Streamlit web interface.

## Prerequisites

- Docker & Docker Compose installed
- 8GB RAM minimum (16GB recommended)
- 10GB free disk space

## Quick Start

### 1. Configure Environment

Create a `.env` file from the template:

```bash
cp .env.example .env
```

Edit `.env` if you need to change database credentials or connect to Supabase.

### 2. Build and Start Services

```bash
docker-compose -f docker-compose.prod.yml up --build
```

This starts three services:
- **milestone2-db**: PostgreSQL 15 + PostGIS
- **milestone2-streamlit**: Web interface (port 8501)
- **milestone2-etl**: ETL processing services

### 3. Access Streamlit Interface

Open your browser and navigate to:
```
http://localhost:8501
```

## Services Overview

### Streamlit Web App (port 8501)
- ETL Pipeline Control
- Interactive Database Explorer
- dbt Transformation Manager
- ML Dashboard (basic)

### ETL Container
Available commands via `docker exec milestone2-etl`:
- `python /app/src/main.py --help` - Full ETL pipeline
- `./entrypoint.sh census` - Census data only
- `./entrypoint.sh urban` - Urban Institute data only
- `./entrypoint.sh location` - Location/geocoding only
- `./entrypoint.sh dbt-run` - Run dbt models
- `./entrypoint.sh test-config` - Test configuration

### Database
- PostgreSQL 15 with PostGIS extension
- Accessible on localhost:5432
- Default credentials in `.env`

## Running ETL Pipelines

### Via Streamlit UI (Recommended)
1. Open http://localhost:8501
2. Navigate to "ETL Control" page
3. Configure year ranges
4. Click "Run Pipeline"

### Via Command Line
```bash
# Complete pipeline
docker exec milestone2-etl python /app/src/main.py

# Census only (2020-2023)
docker exec milestone2-etl python /app/src/main.py --census-only --census-begin-year 2020 --census-end-year 2023

# Urban only
docker exec milestone2-etl python /app/src/main.py --urban-only

# Location only
docker exec milestone2-etl python /app/src/main.py --location-only
```

## Configuration

### Environment Variables (Recommended)
Set in `.env` file:
- `DB_HOST` - Database host (default: db)
- `DB_PORT` - Database port (default: 5432)
- `DB_NAME` - Database name (default: milestone2)
- `DB_USER` - Database user (default: postgres)
- `DB_PASSWORD` - Database password
- `DB_SCHEMA` - Schema name (default: test)
- `DATABASE_TYPE` - local or env (for Supabase)

### Supabase Connection
To connect to Supabase instead of local PostgreSQL:

1. Edit `.env`:
```bash
DATABASE_TYPE=env
DB_HOST=your-project.supabase.co
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your-supabase-password
DB_SCHEMA=public
```

2. Restart services:
```bash
docker-compose -f docker-compose.prod.yml restart
```

## Project Structure

```
milestone2/
├── app/                    # Streamlit web application
│   ├── streamlit_app.py    # Main entry point
│   ├── pages/              # UI pages
│   └── components/         # Reusable components
├── etl/                    # ETL services
│   ├── src/                # ETL modules
│   ├── config/             # Configuration management
│   └── entrypoint.sh       # Container startup script
├── database/               # Database initialization
├── dbt_project/            # Data transformations
├── notebooks/              # Jupyter notebooks (ML)
├── docker-compose.prod.yml # Production orchestration
└── .env                    # Environment configuration
```

## Troubleshooting

### Services won't start
```bash
# Check logs
docker-compose -f docker-compose.prod.yml logs

# Restart services
docker-compose -f docker-compose.prod.yml restart
```

### Database connection failed
```bash
# Check database is running
docker ps | grep milestone2-db

# Test connection
docker exec milestone2-db psql -U postgres -c "SELECT 1"
```

### Streamlit page not loading
```bash
# Check Streamlit logs
docker logs milestone2-streamlit

# Restart Streamlit
docker-compose -f docker-compose.prod.yml restart streamlit
```

### ETL pipeline fails
```bash
# View ETL logs
docker exec milestone2-etl tail -f /app/logs/main_etl.log

# Check ETL container status
docker logs milestone2-etl
```

## Development

### VSCode Dev Containers
The original `.devcontainer` setup is preserved for local development:

1. Open project in VSCode
2. Click "Reopen in Container"
3. Work directly in the development environment

### Running Tests
```bash
# Inside ETL container
docker exec -it milestone2-etl bash
cd /app
pytest etl/tests/
```

## Data Persistence

Data is persisted in Docker volumes:
- `postgres_data`: Database files
- `etl_logs`: ETL execution logs
- `tiger_data`: Downloaded shapefiles
- `model_artifacts`: ML model outputs

To backup data:
```bash
docker run --rm -v milestone2_postgres_data:/data -v $(pwd):/backup ubuntu tar czf /backup/postgres_backup.tar.gz /data
```

## Stopping Services

```bash
# Stop all services
docker-compose -f docker-compose.prod.yml down

# Stop and remove volumes (CAUTION: deletes all data)
docker-compose -f docker-compose.prod.yml down -v
```

## Next Steps

1. Run initial ETL to populate database
2. Execute dbt transformations
3. Explore data via Database Explorer
4. Run ML notebooks for analysis

## Support

- Check logs: `docker-compose -f docker-compose.prod.yml logs [service-name]`
- Interactive shell: `docker exec -it milestone2-etl bash`
- Database client: `docker exec -it milestone2-db psql -U postgres -d milestone2`

## Production Deployment

For production deployment:
1. Use strong passwords in `.env`
2. Enable SSL/TLS for database connections
3. Configure firewall rules (only expose port 8501)
4. Set up automated backups
5. Monitor resource usage
6. Use a reverse proxy (nginx) for HTTPS
