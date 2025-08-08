#!/usr/bin/env python3
"""
Urban Institute Data ETL to PostgreSQL Database - Async Version
==============================================================

This script provides async ETL functionality for Urban Institute API data.
It includes concurrent API calls, improved rate limiting, and enhanced error handling.

Requirements:
- psycopg2-binary
- boto3
- pandas
- sqlalchemy
- aiohttp
- asyncio
- backoff
"""

import sys
import logging
import pandas as pd
import boto3
from sqlalchemy import create_engine, text
import json
from datetime import datetime
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional
import backoff
from dataclasses import dataclass
from pandas import Timestamp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("urban_etl_async.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class TimestampEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle pandas Timestamp objects and NaN values"""

    def default(self, obj):
        if isinstance(obj, Timestamp):
            return obj.isoformat()
        elif pd.isna(obj) or (isinstance(obj, float) and str(obj) == "nan"):
            return None
        return super().default(obj)

    def encode(self, obj):
        """Override encode to handle NaN values in strings"""
        if isinstance(obj, dict):
            # Clean NaN values from dictionary
            cleaned_obj = {}
            for key, value in obj.items():
                if pd.isna(value) or (isinstance(value, float) and str(value) == "nan"):
                    cleaned_obj[key] = None
                elif isinstance(value, str) and value.lower() == "nan":
                    cleaned_obj[key] = None
                else:
                    cleaned_obj[key] = value
            return super().encode(cleaned_obj)
        return super().encode(obj)


@dataclass
class UrbanRequest:
    """Data class for Urban Institute API requests"""

    endpoint: str
    parameters: Dict[str, Any]
    year: Optional[int] = None


class AsyncUrbanDataETL:
    """Async ETL class for Urban Institute data to PostgreSQL"""

    def __init__(self, config_file="config.json", drop_existing_tables=False):
        """Initialize the async ETL process with configuration"""
        self.config = self._load_config(config_file)
        self.db_connection = None
        self.engine = None
        self.session = None
        self.semaphore = None
        self.rate_limiter = None
        self.drop_existing_tables = drop_existing_tables
        self.base_url = self.config.get("urban", {}).get(
            "base_url", "https://educationdata.urban.org"
        )

    def _load_config(self, config_file):
        """Load configuration from JSON file or use defaults"""
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
            logger.info("Configuration loaded successfully")
            return config
        except FileNotFoundError:
            logger.warning(
                f"Configuration file {config_file} not found, using defaults"
            )
            # Return default configuration
            return {
                "database": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "urban_db",
                    "username": "postgres",
                    "password": "password",
                },
                "local_database": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "milestone2",
                    "username": "postgres",
                    "password": "password",
                },
                "urban": {
                    "base_url": "https://educationdata.urban.org",
                    "endpoints": {
                        "schools_directory": "/api/v1/schools/ccd/directory/{year}",
                        "education_students": "/api/v1/schools/crdc/enrollment/{year}/grade-12",
                    },
                },
                "use_aws_secrets": False,
                "max_concurrent_requests": 5,
                "batch_delay": 2,
                "db_batch_size": 1000,
                "async": {
                    "connection_pool_size": 10,
                    "max_overflow": 20,
                    "max_concurrent_requests": 10,
                },
            }

    def _get_aws_secrets(self):
        """Retrieve database credentials from AWS Secrets Manager"""
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name="secretsmanager", region_name=self.config["aws"]["region"]
            )

            secret_name = self.config["aws"]["secret_name"]
            response = client.get_secret_value(SecretId=secret_name)
            secret = json.loads(response["SecretString"])

            logger.info("AWS secrets retrieved successfully")
            return secret
        except Exception as e:
            logger.error(f"Failed to retrieve AWS secrets: {e}")
            raise

    def connect_to_database(self):
        """Establish connection to PostgreSQL database with robust fallback to local"""
        try:
            database_type = self.config.get("database_type", "aws")
            logger.info(f"Attempting to connect to {database_type} database")

            # Try AWS first if configured
            if database_type == "aws":
                try:
                    # Check if AWS configuration exists
                    if "database" not in self.config and "aws" not in self.config:
                        logger.warning(
                            "AWS configuration not found, falling back to local database"
                        )
                        database_type = "local"
                    else:
                        # Get credentials from AWS Secrets Manager or use AWS config
                        if self.config.get("use_aws_secrets", False):
                            db_creds = self._get_aws_secrets()
                        else:
                            db_creds = self.config.get("database", {})

                        # Validate AWS credentials
                        if not db_creds or not all(
                            key in db_creds
                            for key in [
                                "host",
                                "port",
                                "database",
                                "username",
                                "password",
                            ]
                        ):
                            logger.warning(
                                "Incomplete AWS database configuration, falling back to local database"
                            )
                            database_type = "local"
                        else:
                            # Test AWS connection
                            connection_string = (
                                f"postgresql://{db_creds['username']}:{db_creds['password']}"
                                f"@{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
                            )

                            # Get connection pool settings from config
                            async_config = self.config.get("async", {})
                            pool_size = async_config.get("connection_pool_size", 10)
                            max_overflow = async_config.get("max_overflow", 20)

                            # Create SQLAlchemy engine
                            self.engine = create_engine(
                                connection_string,
                                pool_size=pool_size,
                                max_overflow=max_overflow,
                                pool_pre_ping=True,
                                pool_recycle=3600,
                            )

                            # Test connection
                            with self.engine.connect() as conn:
                                conn.execute(text("SELECT 1"))

                            logger.info(
                                "Database connection established successfully to AWS database"
                            )
                            return

                except Exception as aws_error:
                    logger.warning(f"AWS database connection failed: {aws_error}")
                    logger.info("Falling back to local database")
                    database_type = "local"

            # Use local database configuration (either by choice or as fallback)
            if database_type == "local":
                db_creds = self.config.get("local_database", {})

                # Validate local credentials
                if not db_creds or not all(
                    key in db_creds
                    for key in ["host", "port", "database", "username", "password"]
                ):
                    logger.error("Incomplete local database configuration")
                    raise ValueError(
                        "Local database configuration is incomplete. Please check config.json"
                    )

                # Create connection string
                connection_string = (
                    f"postgresql://{db_creds['username']}:{db_creds['password']}"
                    f"@{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
                )

                # Get connection pool settings from config
                async_config = self.config.get("async", {})
                pool_size = async_config.get("connection_pool_size", 10)
                max_overflow = async_config.get("max_overflow", 20)

                # Create SQLAlchemy engine
                self.engine = create_engine(
                    connection_string,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                )

                # Test connection
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))

                logger.info(
                    "Database connection established successfully to local database"
                )
            else:
                raise ValueError(
                    f"Invalid database_type: {database_type}. Must be 'local' or 'aws'"
                )

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def _create_session(self):
        """Create aiohttp session with connection pooling"""
        connector = aiohttp.TCPConnector(
            limit=100, limit_per_host=30, ttl_dns_cache=300, use_dns_cache=True
        )
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": "Urban-Institute-ETL/1.0"},
        )

        # Create semaphore for rate limiting
        max_concurrent = self.config.get("async", {}).get("max_concurrent_requests", 10)
        self.semaphore = asyncio.Semaphore(max_concurrent)

        logger.info(
            f"Created async session with {max_concurrent} concurrent requests limit"
        )

    async def _close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            logger.info("Async session closed")

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=300,
    )
    async def _make_urban_request(
        self, request: UrbanRequest
    ) -> Optional[pd.DataFrame]:
        """Make async request to Urban Institute API with rate limiting"""
        async with self.semaphore:
            try:
                # Build URL with year parameter if specified
                endpoint = request.endpoint
                if request.year and "{year}" in endpoint:
                    endpoint = endpoint.format(year=request.year)

                url = f"{self.base_url}{endpoint}"

                logger.info(f"Making Urban Institute request to: {url}")

                async with self.session.get(url, params=request.parameters) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Convert to DataFrame
                        if isinstance(data, list):
                            df = pd.DataFrame(data)
                        elif isinstance(data, dict):
                            # Handle paginated response format
                            if "results" in data:
                                df = pd.DataFrame(data["results"])
                            elif "data" in data:
                                df = pd.DataFrame(data["data"])
                            else:
                                # If no results/data key, try to use the dict itself
                                df = pd.DataFrame([data])
                        else:
                            df = pd.DataFrame([data])

                        if not df.empty:
                            df["data_source"] = "urban_institute"
                            df["endpoint"] = request.endpoint
                            df["year"] = (
                                request.year if request.year else datetime.now().year
                            )
                            df["fetched_at"] = datetime.now()

                            logger.info(
                                f"Successfully fetched {len(df)} records from {request.endpoint}"
                            )
                            return df
                        else:
                            logger.warning(f"No data returned from {request.endpoint}")
                            return None
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"Urban Institute API error: {response.status} - {error_text}"
                        )

                        # Provide more specific error messages
                        if response.status == 404:
                            logger.error(f"Endpoint not found: {url}")
                            logger.error("Please check if the endpoint URL is correct")
                        elif response.status == 403:
                            logger.error("Access forbidden - API key may be required")
                        elif response.status == 429:
                            logger.error(
                                "Rate limit exceeded - consider reducing request frequency"
                            )
                        else:
                            logger.error(f"Unexpected HTTP status: {response.status}")

                        return None

            except Exception as e:
                logger.error(
                    f"Failed to make Urban Institute request to {request.endpoint}: {e}"
                )
                return None

    async def fetch_urban_data_async(
        self, endpoints: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """Fetch Urban Institute data for specified endpoints"""
        try:
            logger.info(f"Fetching Urban Institute data for {len(endpoints)} endpoints")

            requests = []
            for endpoint_config in endpoints:
                request = UrbanRequest(
                    endpoint=endpoint_config["endpoint"],
                    parameters=endpoint_config.get("parameters", {}),
                    year=endpoint_config.get("year"),
                )
                requests.append(request)

            # Process requests concurrently
            tasks = [self._make_urban_request(req) for req in requests]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Filter out None results and exceptions
            valid_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Urban Institute request {i} failed: {result}")
                elif result is not None:
                    valid_results.append(result)

            if valid_results:
                consolidated_data = pd.concat(valid_results, ignore_index=True)
                consolidated_data.reset_index(drop=True, inplace=True)

                logger.info(
                    f"Total Urban Institute records consolidated: {len(consolidated_data)}"
                )
                return consolidated_data
            else:
                logger.warning("No Urban Institute data was fetched")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Failed to fetch Urban Institute data: {e}")
            raise

    def create_tables(self, drop_existing=False):
        """Create database tables for Urban Institute data"""
        try:
            with self.engine.connect() as conn:
                if drop_existing:
                    # Drop existing tables if requested
                    drop_sql = """
                    DROP TABLE IF EXISTS urban_institute_data CASCADE;
                    DROP TABLE IF EXISTS urban_institute_metadata CASCADE;
                    """
                    conn.execute(text(drop_sql))
                    logger.info("Dropped existing tables")

                # Create urban_institute_data table
                urban_table_sql = """
                CREATE TABLE IF NOT EXISTS urban_institute_data (
                    id SERIAL PRIMARY KEY,
                    data_source VARCHAR(50) DEFAULT 'urban_institute',
                    endpoint VARCHAR(255),
                    year INTEGER,
                    data_json JSONB,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """

                # Create urban_institute_metadata table
                metadata_table_sql = """
                CREATE TABLE IF NOT EXISTS urban_institute_metadata (
                    id SERIAL PRIMARY KEY,
                    endpoint VARCHAR(255) UNIQUE,
                    last_fetched TIMESTAMP,
                    record_count INTEGER,
                    status VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """

                # Create indexes for better performance
                indexes_sql = """
                CREATE INDEX IF NOT EXISTS idx_urban_data_source ON urban_institute_data(data_source);
                CREATE INDEX IF NOT EXISTS idx_urban_data_endpoint ON urban_institute_data(endpoint);
                CREATE INDEX IF NOT EXISTS idx_urban_data_year ON urban_institute_data(year);
                CREATE INDEX IF NOT EXISTS idx_urban_data_json ON urban_institute_data USING GIN(data_json);
                CREATE INDEX IF NOT EXISTS idx_urban_metadata_endpoint ON urban_institute_metadata(endpoint);
                """

                conn.execute(text(urban_table_sql))
                conn.execute(text(metadata_table_sql))
                conn.execute(text(indexes_sql))
                conn.commit()

            logger.info("Database tables created successfully")

        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    async def insert_urban_data_async(self, urban_data: pd.DataFrame):
        """Insert Urban Institute data into database"""
        if urban_data.empty:
            logger.warning("No Urban Institute data to insert")
            return

        try:
            logger.info(
                f"Inserting {len(urban_data)} Urban Institute records into database"
            )

            # Process data in batches
            batch_size = self.config.get("async", {}).get("db_batch_size", 1000)

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                for i in range(0, len(urban_data), batch_size):
                    batch = urban_data.iloc[i : i + batch_size]

                    await loop.run_in_executor(
                        executor, self._insert_urban_batch_sync, batch
                    )

                    logger.info(
                        f"Inserted urban batch {i//batch_size + 1}: {len(batch)} records"
                    )

            logger.info("Successfully inserted Urban Institute data")

        except Exception as e:
            logger.error(f"Failed to insert Urban Institute data: {e}")
            raise

    def _insert_urban_batch_sync(self, batch: pd.DataFrame):
        """Synchronous Urban Institute batch insertion"""
        try:
            # Debug: Print batch info
            logger.info(f"Processing batch with {len(batch)} records")
            logger.info(f"Batch columns: {list(batch.columns)}")
            logger.info(f"Batch dtypes: {batch.dtypes}")

            # Check for any problematic data types
            for col in batch.columns:
                if batch[col].dtype == "object":
                    logger.info(f"Column {col} has object dtype")
                    # Check for mixed types
                    unique_types = set(type(x) for x in batch[col].dropna())
                    logger.info(f"Column {col} has types: {unique_types}")

            # Prepare data for insertion
            insert_data = []

            for idx, row in batch.iterrows():
                try:
                    # Convert row to JSON for storage with custom encoder
                    data_json = row.to_dict()

                    # Clean NaN values from the data
                    cleaned_data_json = {}
                    for key, value in data_json.items():
                        if pd.isna(value) or (
                            isinstance(value, float) and str(value) == "nan"
                        ):
                            cleaned_data_json[key] = None
                        elif isinstance(value, str) and value.lower() == "nan":
                            cleaned_data_json[key] = None
                        else:
                            cleaned_data_json[key] = value

                    # Clean and validate data
                    data_source = str(row.get("data_source", "urban_institute"))
                    endpoint = str(row.get("endpoint", ""))
                    year = row.get("year")

                    # Debug: Print year value and type
                    logger.debug(f"Year value: {year}, type: {type(year)}")

                    # Handle year data type - ensure it's an integer
                    if pd.isna(year) or year is None:
                        year = datetime.now().year
                    else:
                        try:
                            # Convert to string first, then to float, then to int
                            year_str = str(year)
                            year_float = float(year_str)
                            year = int(year_float)
                        except (ValueError, TypeError) as e:
                            logger.warning(
                                f"Could not convert year '{year}' to int: {e}"
                            )
                            year = datetime.now().year

                    # Serialize JSON data
                    try:
                        json_data = json.dumps(cleaned_data_json, cls=TimestampEncoder)
                        logger.debug(
                            f"JSON serialization successful, length: {len(json_data)}"
                        )
                    except Exception as e:
                        logger.error(f"JSON serialization failed: {e}")
                        logger.error(f"Data to serialize: {cleaned_data_json}")
                        raise

                    # Ensure all values are the correct types
                    record = (
                        str(data_source),
                        str(endpoint),
                        int(year),
                        str(json_data),
                    )

                    # Debug: Print record types
                    logger.debug(f"Record types: {[type(x) for x in record]}")

                    insert_data.append(record)

                except Exception as e:
                    logger.error(f"Failed to prepare row {idx}: {e}")
                    logger.error(f"Row data: {row.to_dict()}")
                    logger.error(f"Row types: {row.dtypes}")
                    raise

            # Insert records one by one to avoid batch issues
            with self.engine.connect() as conn:
                insert_sql = """
                INSERT INTO urban_institute_data (data_source, endpoint, year, data_json)
                VALUES (:data_source, :endpoint, :year, :data_json)
                """

                for record in insert_data:
                    data_source, endpoint, year, data_json = record
                    conn.execute(
                        text(insert_sql),
                        {
                            "data_source": data_source,
                            "endpoint": endpoint,
                            "year": year,
                            "data_json": data_json,
                        },
                    )

                conn.commit()

        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")
            raise

    async def update_metadata_async(
        self, endpoint: str, record_count: int, status: str = "success"
    ):
        """Update metadata for endpoint"""
        try:
            with self.engine.connect() as conn:
                upsert_sql = """
                INSERT INTO urban_institute_metadata (endpoint, last_fetched, record_count, status)
                VALUES (:endpoint, :last_fetched, :record_count, :status)
                ON CONFLICT (endpoint)
                DO UPDATE SET
                    last_fetched = EXCLUDED.last_fetched,
                    record_count = EXCLUDED.record_count,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
                """

                conn.execute(
                    text(upsert_sql),
                    {
                        "endpoint": endpoint,
                        "last_fetched": datetime.now(),
                        "record_count": record_count,
                        "status": status,
                    },
                )
                conn.commit()

            logger.info(f"Updated metadata for endpoint: {endpoint}")

        except Exception as e:
            logger.error(f"Failed to update metadata for {endpoint}: {e}")

    async def save_backup_files_async(self, urban_data: pd.DataFrame):
        """Save backup files asynchronously"""
        try:
            if not urban_data.empty:
                logger.info("Saving backup files")

                # Save files in thread pool
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    await loop.run_in_executor(
                        executor,
                        urban_data.to_csv,
                        "urban_institute_data.csv",
                        index=False,
                    )

                logger.info("Backup files saved successfully")

        except Exception as e:
            logger.error(f"Failed to save backup files: {e}")
            raise

    async def run_etl_async(self, endpoints: List[Dict[str, Any]] = None):
        """Run the complete Urban Institute ETL process"""
        try:
            logger.info("Starting Urban Institute ETL process")

            # Step 1: Connect to database
            self.connect_to_database()

            # Step 2: Create tables
            self.create_tables(drop_existing=self.drop_existing_tables)

            # Step 3: Create async session
            await self._create_session()

            try:
                # Step 4: Fetch data from Urban Institute
                if endpoints is None:
                    # Use default endpoints from config
                    urban_config = self.config.get("urban", {})
                    endpoints_config = urban_config.get("endpoints", {})

                    endpoints = [
                        {
                            "endpoint": endpoints_config.get(
                                "schools_directory",
                                "/api/v1/schools/ccd/directory/{year}",
                            ),
                            "parameters": {"limit": 50},
                            "year": 2023,
                        },
                        {
                            "endpoint": endpoints_config.get(
                                "school_characteristics",
                                "/api/v1/schools/ccd/enrollment/{year}/grade-12",
                            ),
                            "parameters": {"limit": 50},
                            "year": 2023,
                        },
                    ]

                urban_data = await self.fetch_urban_data_async(endpoints)

                # Step 5: Insert data into database
                if not urban_data.empty:
                    await self.insert_urban_data_async(urban_data)

                    # Step 6: Update metadata
                    for endpoint_config in endpoints:
                        endpoint = endpoint_config["endpoint"]
                        endpoint_data = urban_data[urban_data["endpoint"] == endpoint]
                        record_count = len(endpoint_data)
                        await self.update_metadata_async(endpoint, record_count)

                # Step 7: Save backup CSV files
                await self.save_backup_files_async(urban_data)

                logger.info("Urban Institute ETL process completed successfully")

            finally:
                # Step 8: Clean up
                await self._close_session()

        except Exception as e:
            logger.error(f"Urban Institute ETL process failed: {e}")
            raise
        finally:
            if self.engine:
                self.engine.dispose()


async def main():
    """Main async function to run the Urban Institute ETL process"""
    try:
        # Load configuration to get default years
        with open("config.json", "r") as f:
            config = json.load(f)

        # Get default years from config
        default_years = config.get("etl", {}).get("urban_years", [2020, 2023])
        default_year = default_years[-1]  # Use the most recent year

        # Initialize ETL process
        etl = AsyncUrbanDataETL()

        # Define Urban Institute endpoints to fetch with year from config
        urban_endpoints = [
            {
                "endpoint": "/api/v1/schools/ccd/directory/{year}",
                "parameters": {"limit": 50},
                "year": default_year,
            },
            {
                "endpoint": "/api/v1/schools/ccd/enrollment/{year}/grade-12",
                "parameters": {"limit": 50},
                "year": default_year,
            },
        ]

        # Run ETL process
        await etl.run_etl_async(endpoints=urban_endpoints)

        print("Urban Institute ETL process completed successfully!")

    except Exception as e:
        logger.error(f"Urban Institute ETL process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
