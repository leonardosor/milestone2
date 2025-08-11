# Modular ETL System for Census and Urban Institute Data

This project provides a comprehensive, modular ETL system that can pull data from multiple APIs and save it into a PostgreSQL database. It supports both US Census Bureau and Urban Institute APIs with async processing for optimal performance.

## Features

- **Modular Design**: Separate components for different data sources
- **Multi-API Support**: Pull data from Census Bureau and Urban Institute APIs
- **Async Processing**: High-performance concurrent API calls
- **AWS Integration**: Support for AWS RDS PostgreSQL and AWS Secrets Manager
- **Comprehensive Error Handling**: Retry logic with exponential backoff
- **Rate Limiting**: Intelligent rate limiting with semaphores
- **Database Schema**: Optimized tables with proper indexes
- **Backup Files**: Automatic CSV backup generation
- **Configurable Data Sources**: Easy to add new API sources
- **QA Mode**: Built-in debugging and testing capabilities

## Project Structure

```
├── main.py                          # Main modular ETL controller
├── census_to_postgresql_async.py    # Census ETL component
├── urban_to_postgresql_async.py     # Urban Institute ETL component
├── run_etl.py                       # ETL runner script with various options
├── debug_etl.py                     # Debug script for testing
├── config.json                      # Configuration file
├── requirements.txt                  # Python dependencies
├── README.md                        # This file
├── main_etl.log                     # Main ETL execution log
├── multi_api_etl.log               # Multi-API ETL execution log
└── .gitignore                       # Git ignore file
```

## Supported APIs

### 1. US Census Bureau API
- **Source**: censusdata Python library
- **Data**: American Community Survey (ACS) 5-year estimates
- **Variables**: Population, demographics, household income
- **Geography**: ZIP code tabulation areas

### 2. Urban Institute API
- **Source**: https://educationdata.urban.org
- **Data**: Education statistics and school information
- **Endpoints**: Schools directory, enrollment data, student demographics
- **Authentication**: No API key required (public API)

## Prerequisites

### AWS Setup

1. **RDS PostgreSQL Instance**
   - Create a PostgreSQL RDS instance on AWS
   - Note the endpoint, port, database name, username, and password
   - Ensure the security group allows connections from your application

2. **AWS Secrets Manager (Optional)**
   - Create a secret in AWS Secrets Manager with database credentials
   - Format: JSON with `username`, `password`, `host`, `port`, `database` keys

3. **IAM Permissions**
   - If using AWS Secrets Manager, ensure your IAM role/user has `secretsmanager:GetSecretValue` permission

### API Setup

1. **Census API Key**
   - Visit: https://api.census.gov/data/key_signup.html
   - Request an API key for the American Community Survey (ACS)

2. **Urban Institute API Key**
   - Visit: https://api.urban.org/
   - Register for an API key
   - Note the available endpoints and rate limits

## Installation

1. **Clone or download the project files**

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the application**
   - Edit `config.json` with your database and API settings
   - Update API keys for both Census and Urban Institute

## Configuration

### Database Setup

The system supports both local PostgreSQL and AWS RDS databases. Database configuration is handled through the `config.json` file.

#### Local Database Setup

1. **Install PostgreSQL locally**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install postgresql postgresql-contrib

   # macOS
   brew install postgresql

   # Windows
   # Download from https://www.postgresql.org/download/windows/
   ```

2. **Create database and user**
   ```sql
   CREATE DATABASE multi_api_data;
   CREATE USER postgres WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE multi_api_data TO postgres;
   ```

3. **Configure local database in config.json**
   ```json
   {
       "database_type": "local",
       "local_database": {
           "host": "localhost",
           "port": 5432,
           "database": "multi_api_data",
           "username": "postgres",
           "password": "your_local_password"
       }
   }
   ```

#### AWS RDS Setup

1. **Set up AWS RDS instance manually or using AWS Console**

2. **Configure AWS database in config.json**
   ```json
   {
       "database_type": "aws",
       "database": {
           "host": "your-aws-rds-endpoint.amazonaws.com",
           "port": 5432,
           "database": "multi_api_data",
           "username": "your_username",
           "password": "your_password"
       },
       "aws": {
           "region": "us-east-1",
           "secret_name": "multi-api-database-credentials"
       },
       "use_aws_secrets": false
   }
   ```

### config.json Structure
    "census": {
        "api_key": "your_census_api_key_here",
        "rate_limit_delay": 1,
        "variables": {
            "B02001_001E": "Total Pop Estimate",
            "B19001_016E": "HHI 150K-200K",
            "B19001_017E": "HHI 220K+",
            "B01001_006E": "Males 15-17",
            "B01001_030E": "Females 15-17",
            "B01001A_006E": "White Males 15-17",
            "B01001B_006E": "Black Males 15-17",
            "B01001I_006E": "Hispanic Males 15-17",
            "B01001A_021E": "White Females 15-17",
            "B01001B_021E": "Black Females 15-17",
            "B01001I_021E": "Hispanic Females 15-17"
        }
    },
    "urban": {
        "api_key": "your_urban_institute_api_key_here",
        "base_url": "https://api.urban.org",
        "endpoints": {
            "education_schools": "/v1/education/schools",
            "education_districts": "/v1/education/districts",
            "education_students": "/v1/education/students",
            "housing": "/v1/housing",
            "health": "/v1/health",
            "crime": "/v1/crime"
        }
    },
    "etl": {
        "batch_size": 1000,
        "census_years": [2015, 2019],
        "urban_years": [2020, 2023]
    },
    "async": {
        "max_concurrent_requests": 10,
        "year_batch_size": 2,
        "batch_delay": 2,
        "locale_batch_size": 100,
        "db_batch_size": 1000,
        "locale_db_batch_size": 500,
        "connection_pool_size": 10,
        "max_overflow": 20,
        "timeout_total": 60,
        "timeout_connect": 10
    }
}
```

### Configuration Options

#### Census Configuration
- **api_key**: Your Census API key
- **rate_limit_delay**: Delay between API calls (seconds)
- **variables**: Census variables to fetch

#### Urban Institute Configuration
- **api_key**: Your Urban Institute API key
- **base_url**: Urban Institute API base URL
- **endpoints**: Available API endpoints

#### Async Configuration
- **max_concurrent_requests**: Maximum concurrent API requests
- **year_batch_size**: Number of years to process in batches
- **batch_delay**: Delay between batches (seconds)
- **db_batch_size**: Database insertion batch size
- **timeout_total**: Total HTTP request timeout (seconds)

## Usage

### Basic Usage

```bash
# Run multi-source ETL (Census + Urban Institute)
python main.py

# Run only Census ETL
python main.py --census-only

# Run only Urban Institute ETL
python main.py --urban-only

# Run with custom Census years
python main.py --census-begin-year 2015 --census-end-year 2019

# Show ETL component status
python main.py --status
```

### Using the ETL Runner Script

```bash
# Run multi-source ETL with config years
python run_etl.py

# Run only Census ETL
python run_etl.py --census-only

# Run only Urban Institute ETL
python run_etl.py --urban-only

# Run with custom years
python run_etl.py --years 2015 2019

# Show configuration
python run_etl.py --show-config

# Show ETL status
python run_etl.py --status

# Run in QA mode with breakpoints
python run_etl.py --qa-mode
```

### Programmatic Usage

```python
from main import ModularETLController
import asyncio

async def main():
    # Initialize ETL controller
    etl_controller = ModularETLController('config.json')

    # Run multi-source ETL
    await etl_controller.run_multi_source_etl()

    # Or run individual components
    await etl_controller.run_census_etl(2015, 2019)
    await etl_controller.run_urban_etl()

    # Check component status
    status = etl_controller.get_etl_status()
    print(f"ETL Status: {status}")

# Run the ETL process
asyncio.run(main())
```

### Debug Mode

```bash
# Run debug script for step-by-step testing
python debug_etl.py
```

### Custom Urban Institute Endpoints

```python
# Example of fetching different Urban Institute data
urban_endpoints = [
    {
        'endpoint': '/api/v1/schools/ccd/directory',
        'parameters': {'limit': 1000},
        'year': 2023
    },
    {
        'endpoint': '/api/v1/schools/ccd/enrollment',
        'parameters': {'limit': 1000},
        'year': 2023
    },
    {
        'endpoint': '/api/v1/schools/crdc/enrollment',
        'parameters': {'limit': 1000},
        'year': 2023
    }
]

# Use with main.py
python main.py --urban-endpoints /api/v1/schools/ccd/directory /api/v1/schools/ccd/enrollment
```

## Database Schema

### census_data Table
- `id`: Primary key
- `zip_code`: ZIP code
- `year`: Year of data
- `total_pop_estimate`: Total population estimate
- `hhi_150k_200k`: Household income $150K-$200K
- `hhi_220k_plus`: Household income $220K+
- `males_15_17`: Males aged 15-17
- `females_15_17`: Females aged 15-17
- `white_males_15_17`: White males aged 15-17
- `black_males_15_17`: Black males aged 15-17
- `hispanic_males_15_17`: Hispanic males aged 15-17
- `white_females_15_17`: White females aged 15-17
- `black_females_15_17`: Black females aged 15-17
- `hispanic_females_15_17`: Hispanic females aged 15-17
- `data_source`: Source identifier ('census')
- `created_at`: Record creation timestamp
- `updated_at`: Record update timestamp

### urban_institute_data Table
- `id`: Primary key
- `data_source`: Source identifier ('urban_institute')
- `year`: Year of data
- `endpoint`: API endpoint used
- `data_json`: JSONB field containing all API response data
- `created_at`: Record creation timestamp
- `updated_at`: Record update timestamp

### locale_data Table
- `id`: Primary key
- `zip_code`: ZIP code (unique)
- `state`: State name
- `city`: City name
- `created_at`: Record creation timestamp
- `updated_at`: Record update timestamp

## Output Files

- `census_data_multi.csv`: Census data backup
- `urban_institute_data.csv`: Urban Institute data backup
- `locale_data_multi.csv`: Location mapping backup
- `multi_api_etl.log`: Detailed execution log

## Performance Features

### Async Processing
- **Concurrent API Calls**: Multiple endpoints processed simultaneously
- **Connection Pooling**: Efficient HTTP and database connection management
- **Rate Limiting**: Intelligent rate limiting with semaphores
- **Error Recovery**: Automatic retry logic with exponential backoff

### Expected Performance Gains
- **3-7x faster** API data retrieval through concurrent processing
- **Reduced memory usage** for large datasets
- **Better error handling** with automatic retries
- **Improved scalability** for processing multiple APIs

## Error Handling

The system includes comprehensive error handling:

- **API Rate Limiting**: Automatic delays between requests
- **Database Connection**: Connection pooling and retry logic
- **Data Validation**: Checks for missing or invalid data
- **Async Error Recovery**: Automatic retry with exponential backoff
- **Multi-Source Handling**: Independent error handling for each API

## Monitoring and Logging

- **Log File**: `multi_api_etl.log` contains detailed execution information
- **Console Output**: Real-time progress updates
- **Error Tracking**: Comprehensive error logging with stack traces
- **Performance Metrics**: Built-in performance monitoring

## Adding New Data Sources

The system is designed to be easily extensible. To add a new data source:

1. **Create a new DataSource class**:
```python
class NewAPIDataSource(DataSource):
    def __init__(self, config):
        self.config = config
        # Initialize your API client

    async def fetch_data(self, request):
        # Implement your API call logic
        pass

    def process_data(self, data):
        # Implement your data processing logic
        pass
```

2. **Register the data source**:
```python
def _initialize_data_sources(self):
    self.data_sources['census'] = CensusDataSource(self.config)
    self.data_sources['urban'] = UrbanInstituteDataSource(self.config)
    self.data_sources['new_api'] = NewAPIDataSource(self.config)  # Add this line
```

3. **Update configuration**:
```json
{
    "new_api": {
        "api_key": "your_api_key",
        "base_url": "https://api.example.com",
        "endpoints": {
            "data": "/v1/data"
        }
    }
}
```

## Troubleshooting

### Common Issues

1. **API Rate Limiting**
   - Increase `rate_limit_delay` in config
   - Reduce `max_concurrent_requests` for async version
   - Check API key validity and limits

2. **Database Connection Issues**
   - Check RDS endpoint and credentials
   - Verify security group allows connections
   - Ensure database exists and is accessible

3. **Urban Institute API Issues**
   - Verify API key is valid
   - Check endpoint URLs are correct
   - Review API documentation for parameter requirements

4. **Memory Issues**
   - Reduce `batch_size` in config
   - Process smaller year ranges
   - Monitor system resources

5. **Async Performance Issues**
   - Adjust `max_concurrent_requests` based on API limits
   - Increase `batch_delay` if rate limiting occurs
   - Monitor connection pool settings

### Debug Mode

Enable debug logging by modifying the logging configuration:

```python
logging.basicConfig(level=logging.DEBUG)
```

## Security Considerations

- **API Key Management**: Store API keys securely
- **AWS Secrets Manager**: Use for database credentials
- **Connection Encryption**: SSL/TLS encryption for all connections
- **Input Validation**: Data validation and sanitization
- **Error Sanitization**: Sensitive information not logged

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the log files
3. Verify API keys and endpoints
4. Create an issue with detailed error information
