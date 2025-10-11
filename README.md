# Enhanced ETL System for Census and Urban Institute Data

This project provides a robust, production-ready ETL system that extracts data from multiple APIs and loads it into a PostgreSQL database. The system has been extensively tested and optimized to handle real-world data challenges including NaN values, timestamp serialization, complex data type conversions, and geographic data processing.

## 🚀 Recent Improvements & Bug Fixes

### ✅ Resolved Issues
- **JSON Serialization Errors**: Fixed "Object of type Timestamp is not JSON serializable" errors
- **NaN Handling**: Resolved "Token 'NaN' is invalid" PostgreSQL JSONB errors
- **Parameter Binding**: Fixed "List argument must consist only of tuples or dictionaries" SQL errors
- **Data Type Conversion**: Resolved "'<' not supported between instances of 'int' and 'str'" errors
- **Enhanced Error Handling**: Comprehensive logging and debugging capabilities
- **Geographic Processing**: Added fast offline geocoding for coordinate-to-location mapping

### 🔧 Technical Enhancements
- **Custom JSON Encoder**: `TimestampEncoder` class for handling pandas Timestamps and NaN values
- **Robust Data Cleaning**: Automatic conversion of problematic data types
- **Improved SQL Parameter Binding**: Named parameters for safer database operations
- **Enhanced Logging**: Detailed debugging information for troubleshooting
- **Data Validation**: Comprehensive data type checking and conversion
- **Modular Architecture**: Organized source code into dedicated modules
- **Jupyter Notebook Support**: Interactive data exploration and table loading tools
- **Geographic Data Processing**: Fast offline geocoding using reverse-geocoder library

## ✨ Key Features

- **Production-Ready**: Extensive error handling and data validation
- **Modular Design**: Separate, well-tested components for different data sources
- **Multi-API Support**: US Census Bureau and Urban Institute APIs
- **Async Processing**: High-performance concurrent API calls with rate limiting
- **Database Flexibility**: Support for both local PostgreSQL and AWS RDS
- **Comprehensive Logging**: Detailed execution logs with debugging information
- **Automatic Data Cleaning**: Handles NaN values, timestamps, and mixed data types
- **Backup Generation**: Automatic CSV backup files for data safety
- **Geographic Processing**: Fast coordinate-to-location mapping
- **Interactive Tools**: Jupyter notebooks for data exploration

## 📁 Project Structure

```
├── src/                             # Source code modules
│   ├── main.py                      # Main ETL controller
│   ├── census_data.py               # Census ETL component
│   ├── urban_data.py                # Urban Institute ETL component
│   ├── location_data.py             # Geographic data processing
│   └── database_explorer.py         # Database exploration utilities
├── dbt_project/                  # dbt (Data Build Tool) project
│   ├── dbt_project.yml           # dbt project configuration
│   ├── profiles.yml              # Database connection profiles
│   ├── models/                   # dbt models
│   │   ├── sources.yml           # Data source definitions
│   │   ├── marts/                # Business logic models
│   │   │   └── dim_school_assessments.sql
│   │   └── staging/              # Raw data staging models
│   │       ├── stg_census_data.sql
│   │       ├── stg_location_data.sql
│   │       ├── stg_school_assessments.sql
│   │       └── stg_school_directory.sql
│
├── run_dbt.ps1                   # PowerShell script for dbt execution
```

## 🧩 Source Code Modules

### Core ETL Components
- **`src/main.py`**: Main ETL controller and orchestration
- **`src/census_data.py`**: US Census Bureau data extraction and processing
- **`src/urban_data.py`**: Urban Institute API data extraction and processing
- **`src/location_data.py`**: Offline geocoding using TIGER/Line shapefiles and GeoPandas
- **`src/database_explorer.py`**: Database connection utilities and exploration tools

### dbt Project Components
- **`dbt_project/models/`**: SQL models for data transformation
  - `staging/`: Raw data cleaning and standardization
  - `marts/`: Business-ready dimensional models
- **`dbt_project/macros/`**: Reusable SQL functions
- **`dbt_project/tests/`**: Data quality and integrity tests
- **`dbt_project/seeds/`**: Static reference data
- **`dbt_project/analyses/`**: Ad-hoc analytical queries

### Automation Scripts
- **`run_dbt.ps1`**: PowerShell script for dbt execution with environment variable support

**Usage:**
```powershell
# Run with environment variables loaded from .env
.\run_dbt.ps1

# Run specific dbt commands
.\run_dbt.ps1 run
.\run_dbt.ps1 test
.\run_dbt.ps1 docs generate
```

**Features:**
- Automatic loading of environment variables from `.env` file
- Support for dbt profiles with database credentials
- Error handling and logging

### Configuration & Environment
- **`config.json`**: Main configuration file for database connections and API settings
- **`.env`**: Environment variables for sensitive credentials (not tracked in git)
- **`.pre-commit-config.yaml`**: Code quality and formatting hooks

## 🗄️ Supported Data Sources
### 1. US Census Bureau API
- **Source**: `censusdata` Python library (v1.15.post1)
- **Data**: American Community Survey (ACS) 5-year estimates
- **Variables**: Population demographics, household income, age distributions
- **Geography**: ZIP code tabulation areas
- **Features**: Automatic retry logic, rate limiting, data validation

### 2. Urban Institute Education Data API
- **Source**: https://educationdata.urban.org
- **Data**: Education statistics, school information, enrollment data
- **Endpoints**:
  - Schools directory: `/api/v1/schools/ccd/directory/{year}`
  - Enrollment data: `/api/v1/schools/ccd/enrollment/{year}/grade-12`
- **Features**: No API key required, comprehensive data cleaning

### 3. Geographic Data Processing (TIGER/Line)
- **Source**: US Census Bureau TIGER/Line shapefiles (2023)
- **Libraries**: GeoPandas, Shapely for spatial operations
- **Data Types**:
  - ZCTA (ZIP Code Tabulation Areas) for ZIP codes
  - County boundaries for county-level data
  - State boundaries for state-level data
- **Features**:
  - Fully offline processing (no API keys required)
  - Authoritative US Census Bureau data
  - Spatial joins for accurate geographic resolution
  - PostGIS geometry storage for advanced spatial queries
  - Automatic CRS transformation to WGS84 (EPSG:4326)
- **Output**: `census_geodata` table with WKT geometries and `location_data` table with ZIP/county/state mappings

## 🛠️ Prerequisites

### System Requirements
- **Python**: 3.8+ (tested with Python 3.9+)
- **PostgreSQL**: 12+ with PostGIS extension (local or AWS RDS)
- **Memory**: Minimum 8GB RAM (16GB+ recommended for large datasets)
- **Storage**: 5GB+ free space for TIGER/Line shapefile downloads

### Database Setup

#### Local PostgreSQL
```bash
# Ubuntu/Debian
sudo apt-get install postgresql postgresql-contrib

# macOS
brew install postgresql

# Windows
# Download from https://www.postgresql.org/download/windows/
```

#### AWS RDS (Optional)
- Future design

## 📦 Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd 696-Milestone-2
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install dbt and additional dependencies**
   ```bash
   pip install -r dbt_project/requirements_dbt.txt
   ```

4. **Install Jupyter (for interactive notebooks)**
   ```bash
   pip install jupyter
   ```

4. **Configure the application**
   - Copy `.env.example` to `.env` and update with your credentials (if available)
   - Edit `config.json` with your database settings
   - Update API keys if required

5. **Set up pre-commit hooks (optional, for development)**
   ```bash
   pip install pre-commit
   pre-commit install
   ```

## 🚀 Usage

### Basic ETL Execution

#### Run Main ETL Controller
```bash
python src/main.py
```

#### Run Individual Components
```bash
# Urban Institute data processing
python src/urban_data.py

# Census data processing
python src/census_data.py

# Geographic data processing (TIGER/Line shapefile geocoding)
python src/location_data.py

# Database exploration
python src/database_explorer.py
```

#### dbt Data Transformation
```bash
# Run dbt models (PowerShell)
.\run_dbt.ps1

# Run dbt models (bash/cmd)
dbt run --profiles-dir dbt_project

# Generate documentation
dbt docs generate --profiles-dir dbt_project
dbt docs serve --profiles-dir dbt_project
```

**Features:**
- Staging layer for data cleaning and standardization
- Marts layer for business-ready dimensional models
- Automated testing for data quality
- Macro functions for reusable SQL logic
- Comprehensive documentation generation

### Data Processing Pipeline
1. **Data Extraction**: Concurrent API calls with rate limiting
2. **Data Cleaning**: Automatic NaN handling and type conversion
3. **JSON Serialization**: Custom encoder for complex data types
4. **Database Insertion**: Batch processing with error handling
5. **Metadata Updates**: Endpoint status and record counts

### Error Handling & Recovery
- **Automatic Retries**: Exponential backoff for failed requests
- **Data Validation**: Comprehensive type checking and conversion
- **Detailed Logging**: Complete error context for debugging

## 📚 Additional Resources

- **Urban Institute API**: https://educationdata.urban.org
- **US Census Bureau**: https://www.census.gov/data/developers.html
- **PostgreSQL Documentation**: https://www.postgresql.org/docs/
- **SQLAlchemy Documentation**: https://docs.sqlalchemy.org/
- **Reverse Geocoder Library**: https://github.com/thampiman/reverse-geocoder

---
