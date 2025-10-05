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
├── logs/                            # Log files directory
│   ├── main_etl.log                 # Main ETL execution log
│   ├── urban_etl_async.log          # Urban Institute ETL log
│   ├── census_etl_async.log         # Census ETL log
│   └── census_etl_simple.log        # Simple Census ETL log
├── outputs/                         # Generated data files
│   ├── census_data_consolidated.csv # Consolidated census data
│   ├── census_raw_async.csv         # Raw census data backup
│   ├── urban_data.csv               # Urban Institute data backup
│   ├── urban_institute_data.csv     # Urban Institute processed data
│   └── zip_to_statecity_async.csv   # Geographic mapping data
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
│   ├── macros/                   # Reusable SQL macros
│   │   └── safe_percentage.sql
│   ├── seeds/                    # Static data files
│   ├── snapshots/                # Slowly changing dimensions
│   ├── analyses/                 # Ad-hoc analysis queries
│   │   └── school_performance_overview.sql
│   ├── tests/                    # Data quality tests
│   │   └── test_math_proficiency_range.sql
│   ├── target/                   # Compiled dbt artifacts
│   ├── logs/                     # dbt execution logs
│   │   └── dbt.log
│   └── requirements_dbt.txt      # dbt-specific dependencies
├── tiger_data/                   # TIGER/Line shapefile data
│   ├── tl_2023_us_county.zip     # County boundaries
│   ├── tl_2023_us_state.zip      # State boundaries
│   ├── tl_2023_us_zcta520.zip    # ZIP Code Tabulation Areas
│   ├── county/                   # Extracted county shapefiles
│   ├── state/                    # Extracted state shapefiles
│   └── zcta/                     # Extracted ZCTA shapefiles
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
- Cross-platform compatibility (PowerShell Core)

### Interactive Tools
- **`database_table_loader.ipynb`**: Interactive Jupyter notebook for loading and exploring database tables
- **`test.ipynb`**: Testing and data exploration notebook

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
- Create PostgreSQL RDS instance
- Configure security groups for access
- Use AWS Secrets Manager for credentials (optional)

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

## ⚙️ Configuration

### config.json Structure
```json
{
    "database_type": "local",
    "local_database": {
        "host": "localhost",
        "port": 5432,
        "database": "milestone2",
        "username": "postgres",
        "password": "your_password"
    },
    "urban": {
        "base_url": "https://educationdata.urban.org",
        "endpoints": {
            "schools_directory": "/api/v1/schools/ccd/directory/{year}",
            "education_students": "/api/v1/schools/ccd/enrollment/{year}/grade-12"
        }
    },
    "etl": {
        "urban_years": [2020, 2023]
    },
    "async": {
        "max_concurrent_requests": 10,
        "db_batch_size": 1000,
        "connection_pool_size": 10,
        "max_overflow": 20
    }
}
```

### Configuration Options

#### Database Configuration
- **database_type**: `"local"` or `"aws"`
- **local_database**: Local PostgreSQL connection details
- **database**: AWS RDS connection details (if using AWS)

#### Urban Institute Configuration
- **base_url**: API base URL (default: educationdata.urban.org)
- **endpoints**: Available API endpoints with year placeholders

#### Async Configuration
- **max_concurrent_requests**: Maximum concurrent API calls
- **db_batch_size**: Database insertion batch size
- **connection_pool_size**: Database connection pool size

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

# Test data quality
dbt test --profiles-dir dbt_project

# Generate documentation
dbt docs generate --profiles-dir dbt_project
dbt docs serve --profiles-dir dbt_project
```

#### Interactive Data Exploration
```bash
# Start Jupyter notebook server
jupyter notebook

# Open either:
# - database_table_loader.ipynb (for database interaction)
# - test.ipynb (for testing and exploration)
```

### Programmatic Usage

```python
from src.urban_data import AsyncUrbanDataETL
from src.location_data import fast_geocode_coordinates
import asyncio

async def main():
    # Initialize ETL process
    etl = AsyncUrbanDataETL()

    # Define endpoints to fetch
    urban_endpoints = [
        {
            'endpoint': '/api/v1/schools/ccd/directory/{year}',
            'parameters': {'limit': 100},
            'year': 2023
        },
        {
            'endpoint': '/api/v1/schools/ccd/enrollment/{year}/grade-12',
            'parameters': {'limit': 100},
            'year': 2023
        }
    ]

    # Run ETL process
    await etl.run_etl_async(endpoints=urban_endpoints)

    # Process geographic data
    fast_geocode_coordinates("location_data")

# Execute
asyncio.run(main())
```

### Geographic Data Processing (TIGER/Line)

```bash
# Test database connection for location data
python src/location_data.py --test-only

# Run full geocoding process with TIGER/Line shapefiles
python src/location_data.py

# Run with custom table name
python src/location_data.py --table-name custom_location_data

# Force download of fresh TIGER shapefiles
python src/location_data.py --download-data
```

**Features:**
- Downloads and processes TIGER/Line shapefiles automatically
- Performs spatial joins to resolve ZIP codes, counties, and states
- Saves all TIGER geometries to `census_geodata` table for future spatial queries
- Supports PostGIS for advanced geographic operations

## 🗃️ Database Schema

### urban_institute_data Table
```sql
CREATE TABLE urban_institute_data (
    id SERIAL PRIMARY KEY,
    data_source VARCHAR(50) DEFAULT 'urban_institute',
    endpoint VARCHAR(255),
    year INTEGER,
    data_json JSONB,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes:**
- `idx_urban_data_source`: On data_source column
- `idx_urban_data_endpoint`: On endpoint column
- `idx_urban_data_year`: On year column
- `idx_urban_data_json`: GIN index on JSONB data

### urban_institute_metadata Table
```sql
CREATE TABLE urban_institute_metadata (
    id SERIAL PRIMARY KEY,
    endpoint VARCHAR(255) UNIQUE,
    last_fetched TIMESTAMP,
    record_count INTEGER,
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### location_data Table
```sql
CREATE TABLE location_data (
    id SERIAL PRIMARY KEY,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    zip VARCHAR(10),
    county VARCHAR(100),
    state VARCHAR(100),
    state_fips VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Purpose**: Complete geocoding results with ZIP codes, counties, and states resolved from TIGER/Line shapefiles.

### census_geodata Table
```sql
CREATE TABLE census_geodata (
    id SERIAL PRIMARY KEY,
    geoid VARCHAR(20),
    name VARCHAR(255),
    layer_type VARCHAR(20),  -- 'zcta', 'county', 'state'
    state_fips VARCHAR(2),
    geometry GEOMETRY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Purpose**: Authoritative TIGER/Line geometries for spatial queries and analysis.
**Indexes:**
- `idx_census_geodata_geoid`: On geoid column
- `idx_census_geodata_layer_type`: On layer_type column
- `idx_census_geodata_state_fips`: On state_fips column
- Spatial index on geometry column (GIST)

## 🔧 Technical Implementation Details

### TIGER/Line Geographic Processing
The system uses authoritative US Census Bureau TIGER/Line shapefiles for accurate geographic resolution:

```python
# Load TIGER/Line shapefiles with GeoPandas
zcta_gdf = gpd.read_file('tiger_data/zcta/tl_2023_us_zcta520.shp')
county_gdf = gpd.read_file('tiger_data/county/tl_2023_us_county.shp')
state_gdf = gpd.read_file('tiger_data/state/tl_2023_us_state.shp')

# Perform spatial joins for geocoding
gdf_pts = gpd.GeoDataFrame(points_df, geometry=points, crs="EPSG:4326")
enriched = gpd.sjoin(gdf_pts, zcta_gdf, predicate="within", how="left")
```

**Key Features:**
- Automatic CRS transformation to WGS84
- Spatial indexing for performance
- PostGIS geometry storage
- Support for ZCTA, county, and state boundaries

### dbt Data Transformation
The project includes a comprehensive dbt setup for data modeling and transformation:

```yaml
# dbt_project.yml configuration
name: 'milestone2_analytics'
version: '1.0.0'
config-version: 2

profile: 'milestone2'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  milestone2_analytics:
    staging:
      materialized: view
    marts:
      materialized: table
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
- **Graceful Degradation**: Fallback values for problematic data
- **Detailed Logging**: Complete error context for debugging

## 📊 Performance Features

### Async Processing Benefits
- **Concurrent API Calls**: 3-7x faster data retrieval
- **Connection Pooling**: Efficient HTTP and database management
- **Rate Limiting**: Intelligent request throttling
- **Batch Processing**: Optimized database insertions

### Memory Management
- **Streaming Processing**: Large datasets processed in chunks
- **Automatic Cleanup**: Resource management and disposal
- **Efficient Data Structures**: Optimized pandas operations

## 📝 Output Files

### Data Outputs (`outputs/` directory)
- **`census_data_consolidated.csv`**: Consolidated and cleaned census data
- **`census_raw_async.csv`**: Raw census data backup from async processing
- **`urban_data.csv`**: Urban Institute data backup
- **`urban_institute_data.csv`**: Processed Urban Institute data
- **`zip_to_statecity_async.csv`**: Geographic mapping data

### TIGER/Line Data (`tiger_data/` directory)
- **`tl_2023_us_zcta520.zip`**: ZIP Code Tabulation Areas shapefile
- **`tl_2023_us_county.zip`**: County boundaries shapefile
- **`tl_2023_us_state.zip`**: State boundaries shapefile
- **`zcta/`**: Extracted ZCTA shapefile components (.shp, .dbf, .prj, .shx)
- **`county/`**: Extracted county shapefile components
- **`state/`**: Extracted state shapefile components

### Log Files (`logs/` directory)
- **`main_etl.log`**: Main ETL execution log
- **`urban_etl_async.log`**: Detailed Urban Institute ETL log
- **`census_etl_async.log`**: Detailed Census ETL log (async version)
- **`census_etl_simple.log`**: Simple Census ETL log

### Configuration Files
- **`config.json`**: Main configuration file
- **`.env`**: Environment variables (credentials, not tracked in git)
- **`requirements.txt`**: Python package dependencies

## 🐛 Troubleshooting

### Common Issues & Solutions

#### 1. JSON Serialization Errors
**Problem**: "Object of type Timestamp is not JSON serializable"
**Solution**: The `TimestampEncoder` class automatically handles this

#### 2. NaN Values in PostgreSQL
**Problem**: "Token 'NaN' is invalid" in JSONB columns
**Solution**: Automatic conversion of NaN to NULL during processing

#### 3. Database Connection Issues
**Problem**: Connection failures or timeouts
**Solution**: Check database credentials and network connectivity

#### 4. Memory Issues
**Problem**: Out of memory errors with large datasets
**Solution**: Reduce batch sizes in configuration

### Debug Mode
Enable detailed logging by modifying the logging level:

```python
logging.basicConfig(level=logging.DEBUG)
```

### Log Analysis
Check log files for detailed error information:
- `urban_etl_async.log`: Urban Institute ETL execution details
- `census_etl_async.log`: Census ETL execution details
- `main_etl.log`: Overall ETL process information

## 🔒 Security Considerations

- **Credential Management**: Secure storage of database credentials
- **API Security**: Rate limiting to prevent API abuse
- **Data Validation**: Input sanitization and validation
- **Connection Encryption**: SSL/TLS for database connections
- **Error Sanitization**: No sensitive information in logs

## 🧪 Testing & Validation

The system has been extensively tested with:
- **Real API Data**: Production Urban Institute and Census APIs
- **Edge Cases**: NaN values, mixed data types, large datasets
- **Error Scenarios**: Network failures, API errors, database issues
- **Performance**: Load testing with concurrent requests

## 📈 Monitoring & Logging

### Log Levels
- **INFO**: General execution information
- **DEBUG**: Detailed debugging information
- **WARNING**: Non-critical issues
- **ERROR**: Error conditions with context

### Performance Metrics
- API response times
- Database insertion rates
- Memory usage
- Error rates and types

## 🚀 Future Enhancements

- **Additional Data Sources**: Support for more APIs
- **Real-time Processing**: Streaming data processing
- **Advanced Analytics**: Built-in data analysis tools
- **Web Dashboard**: Monitoring and control interface
- **Cloud Deployment**: Docker and Kubernetes support

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with comprehensive testing
4. Update documentation
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review log files for error details
3. Verify configuration settings
4. Create an issue with complete error information

## 📚 Additional Resources

- **Urban Institute API**: https://educationdata.urban.org
- **US Census Bureau**: https://www.census.gov/data/developers.html
- **PostgreSQL Documentation**: https://www.postgresql.org/docs/
- **SQLAlchemy Documentation**: https://docs.sqlalchemy.org/
- **Reverse Geocoder Library**: https://github.com/thampiman/reverse-geocoder
- **Jupyter Notebook Documentation**: https://jupyter-notebook.readthedocs.io/

---

**Last Updated**: September 9, 2025
**Version**: 2.2 (Enhanced with TIGER/Line Geospatial Processing & dbt Integration)
**Status**: Production Ready ✅
