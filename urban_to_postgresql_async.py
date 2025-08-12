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
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional
import backoff
from dataclasses import dataclass
from pandas import Timestamp
import numpy as np
import json
from datetime import datetime

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
                        "schools_directory": "/api/v1/schools/crdc/directory/{year}",
                        "education_students": "/api/v1/schools/crdc/enrollment/{year}/grade-12",
                        "test_participation": "/api/v1/schools/crdc/sat-act-participation/{year}/race/sex/",

                    },
                },
                "use_aws_secrets": False,
                "max_concurrent_requests": 5,
                "batch_delay": 2,
                "db_batch_size": 100,
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
        max_tries=5,  # Increased retries for server errors
        max_time=600,  # Increased max time to 10 minutes
    )
    async def _make_urban_request(
        self, request: UrbanRequest
    ) -> Optional[pd.DataFrame]:
        """Make async request to Urban Institute API with rate limiting and pagination"""
        async with self.semaphore:
            try:
                # Build URL with year parameter if specified
                endpoint = request.endpoint
                if request.year and "{year}" in endpoint:
                    endpoint = endpoint.format(year=request.year)

                url = f"{self.base_url}{endpoint}"
                logger.info(f"Making Urban Institute request to: {url}")

                # Initialize variables for pagination
                all_data = []
                page = 1
                
                # Get pagination settings from config
                pagination_config = self.config.get("urban", {}).get("pagination", {})
                page_delay_ms = pagination_config.get("page_delay_ms", 500)
                max_pages = pagination_config.get("max_pages_per_endpoint", 100)
                
                total_records = None
                has_more_pages = True

                while has_more_pages:
                    # Use the current URL (either original or next URL from previous response)
                    current_url = url
                    
                    logger.info(f"Fetching page {page} from: {current_url}")

                    async with self.session.get(current_url, params=request.parameters) as response:
                        if response.status == 200:
                            data = await response.json()

                            # Extract data and pagination info
                            current_df = None
                            if isinstance(data, list):
                                current_df = pd.DataFrame(data)
                                # If it's a list, assume no pagination or single page
                                has_more_pages = False
                            elif isinstance(data, dict):
                                # Handle paginated response format
                                if "results" in data:
                                    current_df = pd.DataFrame(data["results"])
                                    # Check for pagination metadata
                                    if "count" in data:
                                        total_records = data["count"]
                                    # Use the 'next' URL for pagination
                                    if "next" in data and data["next"]:
                                        url = data["next"]  # Update URL for next iteration
                                        has_more_pages = True
                                    else:
                                        has_more_pages = False
                                elif "data" in data:
                                    current_df = pd.DataFrame(data["data"])
                                    # Check for pagination metadata
                                    if "total" in data:
                                        total_records = data["total"]
                                    # Use the 'next' URL for pagination
                                    if "next" in data and data["next"]:
                                        url = data["next"]  # Update URL for next iteration
                                        has_more_pages = True
                                    else:
                                        has_more_pages = False
                                else:
                                    # If no results/data key, try to use the dict itself
                                    current_df = pd.DataFrame([data])
                                    has_more_pages = False
                            else:
                                current_df = pd.DataFrame([data])
                                has_more_pages = False

                            # Add current page data to collection
                            if current_df is not None and not current_df.empty:
                                all_data.append(current_df)
                                logger.info(f"Page {page}: Retrieved {len(current_df)} records")
                                
                                # Check if we've reached the total
                                if total_records is not None:
                                    total_records_fetched = sum(len(df) for df in all_data)
                                    if total_records_fetched >= total_records:
                                        has_more_pages = False
                            else:
                                logger.warning(f"Page {page}: No data returned")
                                has_more_pages = False

                            page += 1

                            # Check if we've exceeded max pages
                            if page > max_pages:
                                logger.warning(f"Reached maximum pages limit ({max_pages}) for {request.endpoint}")
                                has_more_pages = False
                                break

                            # Add delay between pages to be respectful to the API
                            if has_more_pages:
                                await asyncio.sleep(page_delay_ms / 1000)  # Convert ms to seconds

                        else:
                            error_text = await response.text()
                            logger.error(
                                f"Urban Institute API error on page {page}: {response.status} - {error_text}"
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
                                # Wait longer for rate limit
                                if pagination_config.get("retry_on_rate_limit", True):
                                    retry_delay = page_delay_ms * 4 / 1000  # 4x the normal delay
                                    logger.info(f"Waiting {retry_delay}s before retrying...")
                                    await asyncio.sleep(retry_delay)
                                    continue
                                else:
                                    logger.error("Rate limit exceeded and retry is disabled")
                                    return None
                            elif response.status == 500:
                                logger.error(f"Urban Institute server error (500) for {url}")
                                logger.error("This is a server-side issue. Consider:")
                                logger.error("1. Retrying the request later")
                                logger.error("2. Using a different year if available")
                                logger.error("3. Checking Urban Institute API status")
                            elif response.status == 502 or response.status == 503:
                                logger.error(f"Urban Institute service temporarily unavailable ({response.status})")
                                logger.error("Service may be under maintenance or overloaded")
                            else:
                                logger.error(f"Unexpected HTTP status: {response.status}")

                            return None

                # Combine all pages into a single DataFrame
                if all_data:
                    consolidated_df = pd.concat(all_data, ignore_index=True)
                    consolidated_df.reset_index(drop=True, inplace=True)

                    # Add metadata columns
                    consolidated_df["data_source"] = "urban_institute"
                    consolidated_df["endpoint"] = request.endpoint
                    consolidated_df["year"] = (
                        request.year if request.year else datetime.now().year
                    )
                    consolidated_df["fetched_at"] = datetime.now()

                    total_pages = page - 1
                    total_records_fetched = len(consolidated_df)
                    
                    logger.info(
                        f"Successfully fetched {total_records_fetched} total records from {request.endpoint} across {total_pages} pages"
                    )
                    
                    # Log pagination summary
                    if total_pages > 1:
                        avg_records_per_page = total_records_fetched / total_pages
                        logger.info(f"Pagination summary: {avg_records_per_page:.1f} records per page on average")
                        
                        if total_records is not None:
                            coverage = (total_records_fetched / total_records) * 100
                            logger.info(f"Data coverage: {coverage:.1f}% of total available records")
                    
                    return consolidated_df
                else:
                    logger.warning(f"No data returned from {request.endpoint}")
                    return None

            except Exception as e:
                logger.error(
                    f"Failed to make Urban Institute request to {request.endpoint}: {e}"
                )
                return None

    async def _check_year_availability(self, year: int) -> bool:
        """Check if a specific year is available by testing a simple endpoint"""
        try:
            test_endpoint = "/api/v1/schools/ccd/directory/{year}"
            test_url = f"{self.base_url}{test_endpoint.format(year=year)}"
            
            async with self.session.get(test_url, params={"limit": 1}) as response:
                return response.status == 200
        except Exception:
            return False

    async def _get_endpoint_metadata(self, endpoint: str, year: int = None) -> Dict[str, Any]:
        """Get metadata about an endpoint including total record count and pagination info"""
        try:
            # Build URL with year parameter if specified
            if year and "{year}" in endpoint:
                endpoint_url = endpoint.format(year=year)
            else:
                endpoint_url = endpoint

            url = f"{self.base_url}{endpoint_url}"
            
            # Make a minimal request to get metadata
            async with self.session.get(url, params={"limit": 1, "offset": 0}) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    metadata = {
                        "endpoint": endpoint,
                        "year": year,
                        "url": url,
                        "status": "available"
                    }
                    
                    # Extract pagination info
                    if isinstance(data, dict):
                        if "count" in data:
                            metadata["total_records"] = data["count"]
                        elif "total" in data:
                            metadata["total_records"] = data["total"]
                        
                        if "next" in data:
                            metadata["has_pagination"] = bool(data["next"])
                        elif "page" in data and "pages" in data:
                            metadata["has_pagination"] = data["pages"] > 1
                            metadata["total_pages"] = data["pages"]
                    
                    logger.info(f"Endpoint metadata: {metadata}")
                    return metadata
                else:
                    return {
                        "endpoint": endpoint,
                        "year": year,
                        "url": url,
                        "status": "unavailable",
                        "http_status": response.status
                    }
                    
        except Exception as e:
            logger.error(f"Failed to get metadata for endpoint {endpoint}: {e}")
            return {
                "endpoint": endpoint,
                "year": year,
                "status": "error",
                "error": str(e)
            }

    def _get_endpoints_for_year(self, year: int) -> List[Dict[str, Any]]:
        """Get endpoints from config for a specific year"""
        urban_config = self.config.get("urban", {})
        endpoints_config = urban_config.get("endpoints", {})
        
        # Get all endpoints from config dynamically
        all_endpoints = []
        
        for endpoint_key, endpoint_path in endpoints_config.items():
            all_endpoints.append({
                "endpoint": endpoint_path,
                "parameters": {},  # Let the API handle pagination with its default settings
                "year": year,
            })
        
        return all_endpoints

    def _get_all_endpoints_for_years(self, begin_year: int, end_year: int) -> List[Dict[str, Any]]:
        """Get all endpoints for all years in the range"""
        all_endpoints = []
        years_list = list(range(begin_year, end_year + 1))
        
        for year in years_list:
            year_endpoints = self._get_endpoints_for_year(year)
            all_endpoints.extend(year_endpoints)
        
        return all_endpoints

    async def _suggest_alternative_years(self, failed_year: int) -> List[int]:
        """Suggest alternative years to try based on common availability"""
        # Common years that are typically available
        common_years = [2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015]
        
        # Remove the failed year and sort by proximity
        available_years = [y for y in common_years if y != failed_year]
        available_years.sort(key=lambda y: abs(y - failed_year))
        
        return available_years[:3]  # Return top 3 alternatives

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

                # Create urban_institute_data table with base columns
                urban_table_sql = """
                CREATE TABLE IF NOT EXISTS urban_institute_data (
                    id SERIAL PRIMARY KEY,
                    data_source VARCHAR(50) DEFAULT 'urban_institute',
                    endpoint VARCHAR(255),
                    year INTEGER,
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

    def _ensure_columns_exist(self, df: pd.DataFrame):
        """Dynamically add columns to the table based on DataFrame structure"""
        try:
            with self.engine.connect() as conn:
                # Get existing columns
                existing_columns_sql = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'urban_institute_data'
                """
                result = conn.execute(text(existing_columns_sql))
                existing_columns = {row[0] for row in result}

                # Get DataFrame columns (excluding metadata columns)
                metadata_columns = {'data_source', 'endpoint', 'year', 'fetched_at'}
                data_columns = set(df.columns) - metadata_columns

                # Add missing columns
                for col in data_columns:
                    if col not in existing_columns:
                        # Determine column type based on data
                        sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                        
                        if sample_value is not None:
                            if isinstance(sample_value, (int, np.integer)):
                                col_type = "BIGINT"
                            elif isinstance(sample_value, (float, np.floating)):
                                col_type = "DOUBLE PRECISION"
                            elif isinstance(sample_value, bool):
                                col_type = "BOOLEAN"
                            elif isinstance(sample_value, (str, np.character)):
                                # Check if it's a date string
                                try:
                                    pd.to_datetime(sample_value)
                                    col_type = "TIMESTAMP"
                                except:
                                    col_type = "TEXT"
                            else:
                                col_type = "TEXT"
                        else:
                            col_type = "TEXT"

                        add_column_sql = f"ALTER TABLE urban_institute_data ADD COLUMN IF NOT EXISTS \"{col}\" {col_type}"
                        conn.execute(text(add_column_sql))
                        logger.info(f"Added column: {col} ({col_type})")

                conn.commit()
                logger.info("Column structure updated successfully")

        except Exception as e:
            logger.error(f"Failed to ensure columns exist: {e}")
            raise

    def _calculate_optimal_batch_size(self, data_size: int, column_count: int) -> int:
        """Calculate optimal batch size based on data characteristics"""
        # Base batch size from config
        base_batch_size = self.config.get("async", {}).get("db_batch_size", 1000)
        
        # Adjust based on data size and complexity
        if data_size < 100:
            # For small datasets, use smaller batches
            return min(base_batch_size, 500)
        elif column_count > 50:
            # For wide datasets, reduce batch size to avoid memory issues
            return min(base_batch_size, 500)
        elif data_size > 10000:
            # For very large datasets, increase batch size slightly for efficiency
            return min(base_batch_size * 2, 200)
        else:
            return base_batch_size

    async def insert_urban_data_async(self, urban_data: pd.DataFrame):
        """Insert Urban Institute data into database"""
        if urban_data.empty:
            logger.warning("No Urban Institute data to insert")
            return

        try:
            logger.info(
                f"Inserting {len(urban_data)} Urban Institute records into database"
            )

            # Ensure all necessary columns exist in the table
            self._ensure_columns_exist(urban_data)

            # Calculate optimal batch size based on data characteristics
            optimal_batch_size = self._calculate_optimal_batch_size(
                len(urban_data), len(urban_data.columns)
            )
            logger.info(f"Using batch size: {optimal_batch_size} (data: {len(urban_data)} records, {len(urban_data.columns)} columns)")

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                for i in range(0, len(urban_data), optimal_batch_size):
                    batch = urban_data.iloc[i : i + optimal_batch_size]

                    await loop.run_in_executor(
                        executor, self._insert_urban_batch_sync, batch
                    )

                    logger.info(
                        f"Inserted urban batch {i//optimal_batch_size + 1}: {len(batch)} records"
                    )

            logger.info("Successfully inserted Urban Institute data")

        except Exception as e:
            logger.error(f"Failed to insert Urban Institute data: {e}")
            raise

    def _insert_urban_batch_sync(self, batch: pd.DataFrame):
        """Synchronous Urban Institute batch insertion with individual columns"""
        try:
            # Debug: Print batch info
            logger.info(f"Processing batch with {len(batch)} records")
            #logger.info(f"Batch columns: {list(batch.columns)}")
            #logger.info(f"Batch dtypes: {batch.dtypes}")

            # Get metadata columns and data columns
            metadata_columns = {'data_source', 'endpoint', 'year', 'fetched_at'}
            data_columns = [col for col in batch.columns if col not in metadata_columns]
            
            # Prepare column names for SQL (with quotes to handle special characters)
            all_columns = list(metadata_columns) + data_columns
            quoted_columns = [f'"{col}"' for col in all_columns]
            
            # Create placeholders for SQL values
            placeholders = [f':{col}' for col in all_columns]
            
            # Build dynamic INSERT SQL
            insert_sql = f"""
            INSERT INTO urban_institute_data ({', '.join(quoted_columns)})
            VALUES ({', '.join(placeholders)})
            """

            # Insert records one by one to avoid batch issues
            with self.engine.connect() as conn:
                for idx, row in batch.iterrows():
                    try:
                        # Prepare values for insertion
                        values = {}
                        
                        # Handle metadata columns
                        values['data_source'] = str(row.get('data_source', 'urban_institute'))
                        values['endpoint'] = str(row.get('endpoint', ''))
                        
                        # Handle year
                        year = row.get('year')
                        if pd.isna(year) or year is None:
                            year = datetime.now().year
                        else:
                            try:
                                year_str = str(year)
                                year_float = float(year_str)
                                year = int(year_float)
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Could not convert year '{year}' to int: {e}")
                                year = datetime.now().year
                        values['year'] = year
                        
                        # Handle fetched_at
                        fetched_at = row.get('fetched_at')
                        if pd.isna(fetched_at) or fetched_at is None:
                            fetched_at = datetime.now()
                        values['fetched_at'] = fetched_at
                        
                        # Handle data columns
                        for col in data_columns:
                            value = row.get(col)
                            
                            # Clean NaN values
                            if pd.isna(value) or (
                                isinstance(value, float) and str(value) == "nan"
                            ):
                                values[col] = None
                            elif isinstance(value, str) and value.lower() == "nan":
                                values[col] = None
                            else:
                                # Convert numpy types to Python types for database insertion
                                if hasattr(value, 'item'):
                                    values[col] = value.item()
                                else:
                                    values[col] = value
                        
                        # Execute insert
                        conn.execute(text(insert_sql), values)
                        
                    except Exception as e:
                        logger.error(f"Failed to insert row {idx}: {e}")
                        logger.error(f"Row data: {row.to_dict()}")
                        raise

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
                    # Create a wrapper function to call to_csv with index=False
                    def save_csv():
                        return urban_data.to_csv("urban_institute_data.csv", index=False)
                    
                    await loop.run_in_executor(executor, save_csv)

                logger.info("Backup files saved successfully")

        except Exception as e:
            logger.error(f"Failed to save backup files: {e}")
            raise

    async def run_etl_async(self, begin_year: int, end_year: int, endpoints: List[Dict[str, Any]] = None):
        """Run the complete Urban Institute ETL process"""
        try:
            logger.info(f"Starting Urban Institute ETL process for years {begin_year}-{end_year}")

            # Step 1: Connect to database
            self.connect_to_database()

            # Step 2: Create tables
            self.create_tables(drop_existing=self.drop_existing_tables)

            # Step 3: Create async session
            await self._create_session()

            try:
                # Step 4: Fetch data from Urban Institute
                if endpoints is None:
                    # Generate endpoints for all years in the range
                    endpoints = self._get_all_endpoints_for_years(begin_year, end_year)

                # Get metadata for all endpoints to understand pagination requirements
                logger.info("Getting endpoint metadata for pagination planning...")
                metadata_tasks = []
                for endpoint_config in endpoints:
                    task = self._get_endpoint_metadata(
                        endpoint_config["endpoint"], 
                        endpoint_config["year"]
                    )
                    metadata_tasks.append(task)
                
                endpoint_metadata = await asyncio.gather(*metadata_tasks, return_exceptions=True)
                
                # Log metadata summary
                for i, metadata in enumerate(endpoint_metadata):
                    if isinstance(metadata, dict) and metadata.get("status") == "available":
                        total_records = metadata.get("total_records", "unknown")
                        has_pagination = metadata.get("has_pagination", False)
                        logger.info(f"Endpoint {i+1}: {total_records} total records, pagination: {has_pagination}")
                    elif isinstance(metadata, Exception):
                        logger.warning(f"Failed to get metadata for endpoint {i+1}: {metadata}")

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
                else:
                    # If no data was fetched, suggest alternative years
                    if endpoints and endpoints[0].get("year"):
                        failed_year = endpoints[0]["year"]
                        logger.warning(f"No data fetched for year {failed_year}")
                        
                        # Check if the year is available
                        is_available = await self._check_year_availability(failed_year)
                        if not is_available:
                            logger.error(f"Year {failed_year} appears to be unavailable")
                            alternative_years = await self._suggest_alternative_years(failed_year)
                            logger.info(f"Suggested alternative years: {alternative_years}")
                            logger.info("Consider running the ETL process with one of these years")
                        else:
                            logger.info(f"Year {failed_year} appears to be available but returned no data")
                            logger.info("This might be a temporary API issue")

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
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Urban Institute ETL Process")
    parser.add_argument("--begin-year", type=int, required=True, help="Start year for Urban Institute data fetch")
    parser.add_argument("--end-year", type=int, required=True, help="End year for Urban Institute data fetch")
    parser.add_argument("--config", type=str, default="config.json", help="Configuration file path")
    args = parser.parse_args()
    
    try:
        # Initialize ETL process
        etl = AsyncUrbanDataETL(config_file=args.config)

        # Run ETL process with year range
        await etl.run_etl_async(begin_year=args.begin_year, end_year=args.end_year)

        logger.info("Urban Institute ETL process completed successfully!")

    except Exception as e:
        logger.error(f"Urban Institute ETL process failed: {e}")
        
        # Provide helpful suggestions for common failures
        if "500" in str(e) or "Server Error" in str(e):
            logger.error("\n" + "="*60)
            logger.error("URBAN INSTITUTE API SERVER ERROR DETECTED")
            logger.error("="*60)
            logger.error("This is a server-side issue on Urban Institute's end.")
            logger.error("Recommendations:")
            logger.error("1. Wait a few minutes and try again")
            logger.error("2. Try a different year (e.g., 2022, 2021, 2020)")
            logger.error("3. Check Urban Institute API status page")
            logger.error("4. The API may be under maintenance")
            logger.error("="*60)
        
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
