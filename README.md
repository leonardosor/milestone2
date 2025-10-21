# Enhanced ETL System for Census and Urban Institute Data

This project provides a production-ready ETL system that extracts data from multiple APIs and loads it into a PostgreSQL database with comprehensive error handling and data validation.

## Recent Improvements

- Fixed JSON serialization errors with Timestamp objects
- Resolved NaN handling in PostgreSQL JSONB operations
- Fixed SQL parameter binding issues
- Enhanced error handling and logging
- Added fast offline geocoding for coordinate mapping

## Key Features

- Multi-API support for US Census Bureau and Urban Institute
- Modular design with separate components for different data sources
- Support for local PostgreSQL and AWS RDS
- Automatic data cleaning and NaN handling
- CSV backup generation
- Fast offline geographic processing
- Interactive Jupyter notebook tools

## Project Structure

```
├── src/                             # Source code modules
│   ├── main.py                      # Main ETL controller
│   ├── census_data.py               # Census ETL component
│   ├── urban_data.py                # Urban Institute ETL component
│   ├── location_data.py             # Geographic data processing
│   └── database_explorer.py         # Database utilities
├── dbt_project/                     # dbt transformation project
│   ├── models/                      # SQL transformation models
│   │   ├── staging/                 # Data cleaning models
│   │   └── marts/                   # Business logic models
│   └── dbt_project.yml              # dbt configuration
└── run_dbt.ps1                      # PowerShell script for dbt
```

## Data Sources

### US Census Bureau API
- American Community Survey (ACS) 5-year estimates
- Demographics, household income, age distributions
- ZIP code tabulation areas

### Urban Institute Education Data API
- Education statistics and school information
- Endpoints: schools directory and enrollment data
- No API key required

### Geographic Data (TIGER/Line)
- US Census Bureau shapefiles (2023)
- Offline processing with GeoPandas
- County, state, and ZIP code boundaries

## Prerequisites

- Python 3.8+
- PostgreSQL 12+ with PostGIS
- 8GB RAM minimum (16GB recommended)
- 5GB free storage for shapefiles

## Installation

1. Clone the repository
   ```bash
   git clone <repository-url>
   cd 696-Milestone-2
   ```

2. Install Python dependencies
   ```bash
   pip install -r requirements.txt
   pip install -r dbt_project/requirements_dbt.txt
   ```

3. Configure the application
   - Copy `.env.example` to `.env` with your credentials
   - Edit `config.json` with database settings

4. Install pre-commit hooks (optional)
   ```bash
   pip install pre-commit
   pre-commit install
   ```

## Usage

### ETL Execution

```bash
# Run main ETL controller
python src/main.py

# Run individual components
python src/urban_data.py
python src/census_data.py
python src/location_data.py
```

### dbt Transformation

```bash
# PowerShell
.\run_dbt.ps1

# Bash/CMD
dbt run --profiles-dir dbt_project
dbt docs generate --profiles-dir dbt_project
```

## Resources

- Urban Institute API: https://educationdata.urban.org
- US Census Bureau: https://www.census.gov/data/developers.html
- PostgreSQL Documentation: https://www.postgresql.org/docs/
