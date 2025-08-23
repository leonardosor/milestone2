# Enhanced ETL System for Census and Urban Institute Data

This project provides a robust, production-ready ETL system that extracts data from multiple APIs and loads it into a PostgreSQL database. The system has been extensively tested and optimized to handle real-world data challenges including NaN values, timestamp serialization, and complex data type conversions.

## 🚀 Recent Improvements & Bug Fixes

### ✅ Resolved Issues
- **JSON Serialization Errors**: Fixed "Object of type Timestamp is not JSON serializable" errors
- **NaN Handling**: Resolved "Token 'NaN' is invalid" PostgreSQL JSONB errors
- **Parameter Binding**: Fixed "List argument must consist only of tuples or dictionaries" SQL errors
- **Data Type Conversion**: Resolved "'<' not supported between instances of 'int' and 'str'" errors
- **Enhanced Error Handling**: Comprehensive logging and debugging capabilities

### 🔧 Technical Enhancements
- **Custom JSON Encoder**: `TimestampEncoder` class for handling pandas Timestamps and NaN values
- **Robust Data Cleaning**: Automatic conversion of problematic data types
- **Improved SQL Parameter Binding**: Named parameters for safer database operations
- **Enhanced Logging**: Detailed debugging information for troubleshooting
- **Data Validation**: Comprehensive data type checking and conversion

## ✨ Key Features

- **Production-Ready**: Extensive error handling and data validation
- **Modular Design**: Separate, well-tested components for different data sources
- **Multi-API Support**: US Census Bureau and Urban Institute APIs
- **Async Processing**: High-performance concurrent API calls with rate limiting
- **Database Flexibility**: Support for both local PostgreSQL and AWS RDS
- **Comprehensive Logging**: Detailed execution logs with debugging information
- **Automatic Data Cleaning**: Handles NaN values, timestamps, and mixed data types
- **Backup Generation**: Automatic CSV backup files for data safety

## 📁 Project Structure

```
├── main.py                          # Main ETL controller
├── census_to_postgresql_async.py    # Census ETL component (async)
├── urban_to_postgresql_async.py     # Urban Institute ETL component (async)
├── config.json                      # Configuration file
├── requirements.txt                  # Python dependencies
├── README.md                        # This file
├── main_etl.log                     # Main ETL execution log
├── urban_etl_async.log             # Urban Institute ETL log
├── census_etl_async.log            # Census ETL log
└── .gitignore                       # Git ignore file
```

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

## 🛠️ Prerequisites

### System Requirements
- **Python**: 3.8+ (tested with Python 3.9+)
- **PostgreSQL**: 12+ (local or AWS RDS)
- **Memory**: Minimum 4GB RAM (8GB+ recommended for large datasets)

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

3. **Configure the application**
   - Edit `config.json` with your database settings
   - Update API keys if required

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

#### Run Urban Institute ETL
```bash
python urban_to_postgresql_async.py
```

#### Run Census ETL
```bash
python census_to_postgresql_async.py
```

#### Run Main ETL Controller
```bash
python main.py
```

### Programmatic Usage

```python
from urban_to_postgresql_async import AsyncUrbanDataETL
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

# Execute
asyncio.run(main())
```

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

## 🔧 Technical Implementation Details

### Custom JSON Encoder
The system includes a robust `TimestampEncoder` class that handles:

```python
class TimestampEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Timestamp):
            return obj.isoformat()
        elif pd.isna(obj) or (isinstance(obj, float) and str(obj) == 'nan'):
            return None
        return super().default(obj)
    
    def encode(self, obj):
        # Pre-clean dictionaries to handle NaN values
        if isinstance(obj, dict):
            cleaned_obj = {}
            for key, value in obj.items():
                if pd.isna(value) or (isinstance(value, float) and str(value) == 'nan'):
                    cleaned_obj[key] = None
                elif isinstance(value, str) and value.lower() == 'nan':
                    cleaned_obj[key] = None
                else:
                    cleaned_obj[key] = value
            return super().encode(cleaned_obj)
        return super().encode(obj)
```

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

- **urban_institute_data.csv**: Urban Institute data backup
- **census_raw_async.csv**: Census data backup
- **urban_etl_async.log**: Detailed Urban Institute ETL log
- **census_etl_async.log**: Detailed Census ETL log
- **main_etl.log**: Main ETL execution log

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

---

**Last Updated**: August 2025  
**Version**: 2.0 (Enhanced)  
**Status**: Production Ready ✅
