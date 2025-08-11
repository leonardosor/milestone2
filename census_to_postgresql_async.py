#!/usr/bin/env python3
"""
Census Data ETL to PostgreSQL Database on AWS - Async Version (Fixed)
======================================================================

This script extends the Census API data pull functionality with async methods for better performance.
It includes concurrent API calls, improved rate limiting, and enhanced error handling.

Requirements:
- psycopg2-binary
- boto3
- censusdata
- pgeocode
- pandas
- sqlalchemy
- aiohttp
- asyncio
- backoff
"""

import sys
import logging
import pandas as pd
import censusdata
import pgeocode
import boto3
from sqlalchemy import create_engine, text
import json
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
import backoff
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("census_etl_async.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


@dataclass
class CensusRequest:
    """Data class for Census API requests"""

    year: int
    variables: List[str]
    geography: str
    state: str = "*"
    zip_code: str = "*"


class AsyncCensusDataETL:
    """Async ETL class for Census data to PostgreSQL on AWS"""

    def __init__(self, config_file="config.json", drop_existing_tables=False):
        """Initialize the async ETL process with configuration"""
        self.config = self._load_config(config_file)
        self.db_connection = None
        self.engine = None
        self.session = None
        self.semaphore = None
        self.rate_limiter = None
        self.drop_existing_tables = drop_existing_tables

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
                    "database": "census_db",
                    "username": "postgres",
                    "password": "password",
                },
                "local_database": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "census_db",
                    "username": "postgres",
                    "password": "password",
                },
                "use_aws_secrets": False,
                "max_concurrent_requests": 5,
                "year_batch_size": 2,
                "batch_delay": 2,
                "locale_batch_size": 50,
                "db_batch_size": 1000,
                "locale_db_batch_size": 500,
            }
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise

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

                            # Create SQLAlchemy engine
                            self.engine = create_engine(
                                connection_string,
                                pool_size=10,
                                max_overflow=20,
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

                # Create SQLAlchemy engine
                self.engine = create_engine(
                    connection_string,
                    pool_size=10,
                    max_overflow=20,
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
            headers={"User-Agent": "Census-Data-ETL/1.0"},
        )

        # Create semaphore for rate limiting
        max_concurrent = self.config.get("max_concurrent_requests", 5)
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
        Exception,  # Catch broader exceptions for Census API issues
        max_tries=3,
        max_time=300,
    )
    async def _make_census_request(
        self, request: CensusRequest
    ) -> Optional[pd.DataFrame]:
        """Make a single Census API request with retry logic"""
        async with self.semaphore:
            try:
                logger.info(f"Making Census request for year {request.year}")

                # Use the synchronous censusdata library in a thread pool
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    census_data = await loop.run_in_executor(
                        executor, self._sync_census_request, request
                    )

                if census_data is not None and not census_data.empty:
                    logger.info(
                        f"Successfully pulled {len(census_data)} records for year {request.year}"
                    )
                    return census_data
                else:
                    logger.warning(f"No data returned for year {request.year}")
                    return None

            except Exception as e:
                logger.error(
                    f"Failed to make Census request for year {request.year}: {e}"
                )
                raise

    def _sync_census_request(self, request: CensusRequest) -> Optional[pd.DataFrame]:
        """Synchronous Census API request (runs in thread pool)"""
        try:
            # Census variables dictionary
            dictionary = {
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
                "B01001I_021E": "Hispanic Females 15-17",
            }

            llaves = sorted(list(dictionary.keys()), reverse=True)

            census_data = censusdata.download(
                "acs5",
                request.year,
                censusdata.censusgeo([("zip code tabulation area", "*")]),
                llaves,
            )

            census_data.rename(columns=dictionary, inplace=True)
            census_data.reset_index(inplace=True)

            # Extract zip codes properly from the CensusGeo objects
            census_data["zip code"] = census_data["index"].apply(
                lambda x: x.params()[0][1]
                if hasattr(x, "params") and x.params()
                else str(x)
            )
            census_data["year"] = request.year
            census_data.drop(columns="index", inplace=True)

            return census_data

        except Exception as e:
            logger.error(f"Sync Census request failed for year {request.year}: {e}")
            return None

    async def _process_year_batch(self, years: List[int]) -> List[pd.DataFrame]:
        """Process a batch of years concurrently"""
        tasks = []

        for year in years:
            request = CensusRequest(
                year=year,
                variables=[],  # Will use default variables
                geography="zip code tabulation area",
            )
            task = self._make_census_request(request)
            tasks.append(task)

        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out None results and exceptions
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task for year {years[i]} failed: {result}")
            elif result is not None:
                valid_results.append(result)

        return valid_results

    async def api_iterator_async(self, begin_year: int, end_year: int) -> pd.DataFrame:
        """Async iterator over years and pull Census data with batching"""
        try:
            # Create batches of years for concurrent processing
            batch_size = self.config.get("year_batch_size", 2)
            years_list = list(
                range(begin_year, end_year + 1)
            )  # Fixed: include end_year
            year_batches = [
                years_list[i : i + batch_size]
                for i in range(0, len(years_list), batch_size)
            ]

            logger.info(
                f"Processing {len(year_batches)} batches of years: {years_list}"
            )

            all_results = []

            for i, year_batch in enumerate(year_batches):
                logger.info(
                    f"Processing batch {i+1}/{len(year_batches)}: years {year_batch}"
                )

                batch_results = await self._process_year_batch(year_batch)
                all_results.extend(batch_results)

                # Add delay between batches to avoid overwhelming the API
                if i < len(year_batches) - 1:
                    delay = self.config.get("batch_delay", 2)
                    logger.info(f"Waiting {delay} seconds before next batch...")
                    await asyncio.sleep(delay)

            if all_results:
                consolidated_data = pd.concat(all_results, ignore_index=True)
                consolidated_data.reset_index(drop=True, inplace=True)

                logger.info(f"Total records consolidated: {len(consolidated_data)}")
                return consolidated_data
            else:
                logger.warning("No data was pulled from the API")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Failed to iterate through years: {e}")
            raise

    async def create_locale_data_async(self, census_data: pd.DataFrame) -> pd.DataFrame:
        """Create location mapping data with async processing using pgeocode"""
        try:
            logger.info("Creating locale data mapping using pgeocode")

            unique_zips = pd.DataFrame(census_data["zip code"].drop_duplicates())

            # Process zip codes in batches
            batch_size = self.config.get("locale_batch_size", 50)
            zip_batches = [
                unique_zips["zip code"].iloc[i : i + batch_size].tolist()
                for i in range(0, len(unique_zips), batch_size)
            ]

            all_states = []
            all_cities = []

            for i, zip_batch in enumerate(zip_batches):
                logger.info(f"Processing locale batch {i+1}/{len(zip_batches)}")

                # Process batch in thread pool
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    batch_results = await loop.run_in_executor(
                        executor, self._process_zip_batch, zip_batch
                    )

                all_states.extend(batch_results["states"])
                all_cities.extend(batch_results["cities"])

                # Small delay between batches
                await asyncio.sleep(0.1)

            unique_zips["state"] = all_states
            unique_zips["city"] = all_cities

            logger.info(
                f"Created locale mapping for {len(unique_zips)} unique zip codes"
            )
            return unique_zips

        except Exception as e:
            logger.error(f"Failed to create locale data: {e}")
            raise

    def _process_zip_batch(self, zip_codes: List[str]) -> Dict[str, List[str]]:
        """Process a batch of zip codes using pgeocode (runs in thread pool)"""
        states = []
        cities = []

        # Initialize pgeocode for US zip codes inside the thread
        geo_coder = pgeocode.Nominatim("us")

        for zip_code in zip_codes:
            try:
                # Clean zip code
                clean_zip = str(zip_code).strip()

                # Query location data using pgeocode
                location = geo_coder.query_postal_code(clean_zip)

                # Extract state and city information
                if location is not None and not pd.isna(location.state_code):
                    state = location.state_code
                    city = (
                        location.place_name
                        if not pd.isna(location.place_name)
                        else None
                    )
                else:
                    state = None
                    city = None

                states.append(state)
                cities.append(city)

            except Exception as e:
                logger.debug(f"Failed to process zip code {zip_code}: {e}")
                states.append(None)
                cities.append(None)

        return {"states": states, "cities": cities}

    def create_tables(self, drop_existing=False):
        """Create database tables, optionally dropping existing ones first"""
        try:
            with self.engine.connect() as conn:
                if drop_existing:
                    logger.info("Dropping existing tables...")
                    # Drop tables in correct order (child tables first due to potential foreign keys)
                    drop_tables_sql = """
                    DROP TABLE IF EXISTS census_data CASCADE;
                    DROP TABLE IF EXISTS locale_data CASCADE;
                    """
                    conn.execute(text(drop_tables_sql))
                    conn.commit()
                    logger.info("Existing tables dropped successfully")
                else:
                    # Check if tables exist and have the correct schema
                    logger.info("Checking existing table schemas...")
                    try:
                        # Check if census_data table has state and city columns
                        check_schema_sql = """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'census_data'
                        AND column_name IN ('state', 'city')
                        """
                        result = conn.execute(text(check_schema_sql))
                        existing_columns = [row[0] for row in result]

                        if len(existing_columns) < 2:
                            logger.warning(
                                "Existing census_data table missing state/city columns, dropping and recreating..."
                            )
                            drop_tables_sql = """
                            DROP TABLE IF EXISTS census_data CASCADE;
                            DROP TABLE IF EXISTS locale_data CASCADE;
                            """
                            conn.execute(text(drop_tables_sql))
                            conn.commit()
                            drop_existing = True  # Force recreation
                        else:
                            logger.info("Existing tables have correct schema")

                    except Exception as schema_check_error:
                        logger.warning(
                            f"Could not check table schema: {schema_check_error}"
                        )
                        # If we can't check, assume we need to recreate
                        drop_existing = True

                # Create census_data table
                census_table_sql = """
                CREATE TABLE IF NOT EXISTS census_data (
                    id SERIAL PRIMARY KEY,
                    zip_code VARCHAR(10),
                    state VARCHAR(2),
                    city VARCHAR(100),
                    year INTEGER,
                    total_pop_estimate INTEGER,
                    hhi_150k_200k INTEGER,
                    hhi_220k_plus INTEGER,
                    males_15_17 INTEGER,
                    females_15_17 INTEGER,
                    white_males_15_17 INTEGER,
                    black_males_15_17 INTEGER,
                    hispanic_males_15_17 INTEGER,
                    white_females_15_17 INTEGER,
                    black_females_15_17 INTEGER,
                    hispanic_females_15_17 INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """

                # Create locale_data table
                locale_table_sql = """
                CREATE TABLE IF NOT EXISTS locale_data (
                    id SERIAL PRIMARY KEY,
                    zip_code VARCHAR(10) UNIQUE,
                    state VARCHAR(50),
                    city VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """

                # Create indexes for better performance
                indexes_sql = """
                CREATE INDEX IF NOT EXISTS idx_census_data_zip_year ON census_data(zip_code, year);
                CREATE INDEX IF NOT EXISTS idx_census_data_state ON census_data(state);
                CREATE INDEX IF NOT EXISTS idx_census_data_state_year ON census_data(state, year);
                CREATE INDEX IF NOT EXISTS idx_locale_data_zip ON locale_data(zip_code);
                CREATE INDEX IF NOT EXISTS idx_locale_data_state ON locale_data(state);
                """

                conn.execute(text(census_table_sql))
                conn.execute(text(locale_table_sql))

                # Add missing columns if table already existed
                try:
                    alter_census_sql = """
                    ALTER TABLE census_data
                    ADD COLUMN IF NOT EXISTS state VARCHAR(2),
                    ADD COLUMN IF NOT EXISTS city VARCHAR(100);
                    """
                    conn.execute(text(alter_census_sql))

                    alter_locale_sql = """
                    ALTER TABLE locale_data
                    ADD COLUMN IF NOT EXISTS state VARCHAR(50),
                    ADD COLUMN IF NOT EXISTS city VARCHAR(100);
                    """
                    conn.execute(text(alter_locale_sql))

                except Exception as alter_error:
                    logger.warning(
                        f"Could not alter tables (this is normal for new tables): {alter_error}"
                    )

                conn.execute(text(indexes_sql))
                conn.commit()

            logger.info("Database tables created successfully")

        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    async def merge_census_with_locale(
        self, census_data: pd.DataFrame, locale_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge census data with locale data to add state and city columns"""
        try:
            logger.info("Merging census data with locale information")

            # Merge census data with locale data on zip code
            census_enhanced = census_data.merge(
                locale_data[["zip code", "state", "city"]], on="zip code", how="left"
            )

            # Log merge statistics
            total_records = len(census_enhanced)
            records_with_state = len(census_enhanced[census_enhanced["state"].notna()])
            records_with_city = len(census_enhanced[census_enhanced["city"].notna()])

            logger.info(f"Merge completed: {total_records} total records")
            logger.info(
                f"Records with state: {records_with_state} ({records_with_state/total_records*100:.1f}%)"
            )
            logger.info(
                f"Records with city: {records_with_city} ({records_with_city/total_records*100:.1f}%)"
            )

            # Show sample of merged data
            logger.info("Sample of merged data:")
            sample_data = census_enhanced[
                ["zip code", "state", "city", "year", "Total Pop Estimate"]
            ].head()
            logger.info(f"\n{sample_data.to_string()}")

            return census_enhanced

        except Exception as e:
            logger.error(f"Failed to merge census with locale data: {e}")
            raise

    async def insert_census_data_async(self, census_data: pd.DataFrame):
        """Insert census data into database with async batching"""
        try:
            logger.info(f"Inserting {len(census_data)} census records into database")

            # Prepare data for insertion
            census_data_db = census_data.copy()

            # Rename columns to match database schema
            column_mapping = {
                "zip code": "zip_code",
                "Total Pop Estimate": "total_pop_estimate",
                "HHI 150K-200K": "hhi_150k_200k",
                "HHI 220K+": "hhi_220k_plus",
                "Males 15-17": "males_15_17",
                "Females 15-17": "females_15_17",
                "White Males 15-17": "white_males_15_17",
                "Black Males 15-17": "black_males_15_17",
                "Hispanic Males 15-17": "hispanic_males_15_17",
                "White Females 15-17": "white_females_15_17",
                "Black Females 15-17": "black_females_15_17",
                "Hispanic Females 15-17": "hispanic_females_15_17",
                # state and city columns are already named correctly from locale merge
            }

            census_data_db.rename(columns=column_mapping, inplace=True)

            # Insert data in batches
            batch_size = self.config.get("db_batch_size", 1000)
            total_inserted = 0

            # Process batches in thread pool
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                for i in range(0, len(census_data_db), batch_size):
                    batch = census_data_db.iloc[i : i + batch_size]

                    await loop.run_in_executor(
                        executor, self._insert_batch_sync, batch, "census_data"
                    )

                    total_inserted += len(batch)
                    logger.info(
                        f"Inserted batch {i//batch_size + 1}: {len(batch)} records"
                    )

            logger.info(f"Successfully inserted {total_inserted} census records")

        except Exception as e:
            logger.error(f"Failed to insert census data: {e}")
            raise

    def _insert_batch_sync(self, batch: pd.DataFrame, table_name: str):
        """Synchronous batch insertion (runs in thread pool)"""
        with self.engine.connect() as conn:
            batch.to_sql(
                table_name, conn, if_exists="append", index=False, method="multi"
            )
            conn.commit()

    async def insert_locale_data_async(self, locale_data: pd.DataFrame):
        """Insert locale data into database with async processing"""
        try:
            logger.info(f"Inserting {len(locale_data)} locale records into database")

            # Prepare data for insertion
            locale_data_db = locale_data.copy()
            locale_data_db.rename(columns={"zip code": "zip_code"}, inplace=True)

            # Process in batches
            batch_size = self.config.get("locale_db_batch_size", 500)

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                for i in range(0, len(locale_data_db), batch_size):
                    batch = locale_data_db.iloc[i : i + batch_size]

                    await loop.run_in_executor(
                        executor, self._insert_batch_sync, batch, "locale_data"
                    )

                    logger.info(
                        f"Inserted locale batch {i//batch_size + 1}: {len(batch)} records"
                    )

            logger.info("Successfully inserted locale data")

        except Exception as e:
            logger.error(f"Failed to insert locale data: {e}")
            raise

    async def save_backup_files_async(
        self, census_data: pd.DataFrame, locale_data: pd.DataFrame
    ):
        """Save backup files asynchronously"""
        try:
            logger.info("Saving backup files")

            # Save files in thread pool
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                await asyncio.gather(
                    loop.run_in_executor(
                        executor, census_data.to_csv, "census_raw_async.csv"
                    ),
                    loop.run_in_executor(
                        executor, locale_data.to_csv, "zip_to_statecity_async.csv"
                    ),
                )

            logger.info("Backup files saved successfully")

        except Exception as e:
            logger.error(f"Failed to save backup files: {e}")
            raise

    async def run_etl_async(self, begin_year: int = 2015, end_year: int = 2024):
        """Run the complete async ETL process"""
        try:
            logger.info("Starting async ETL process")

            # Step 1: Connect to database
            self.connect_to_database()

            # Step 2: Create tables (drop existing if requested)
            self.create_tables(drop_existing=self.drop_existing_tables)

            # Step 3: Create async session
            await self._create_session()

            try:
                # Step 4: Pull Census data
                census_data = await self.api_iterator_async(begin_year, end_year)

                if census_data.empty:
                    logger.warning("No census data to process")
                    return

                # Step 5: Create locale data and merge with census data
                locale_data = await self.create_locale_data_async(census_data)

                # Step 6: Merge locale data (state/city) with census data
                census_data_enhanced = await self.merge_census_with_locale(
                    census_data, locale_data
                )

                # Step 7: Insert data into database
                await asyncio.gather(
                    self.insert_census_data_async(census_data_enhanced),
                    self.insert_locale_data_async(locale_data),
                )

                # Step 8: Save backup CSV files
                await self.save_backup_files_async(census_data_enhanced, locale_data)

                logger.info("Async ETL process completed successfully")

            finally:
                # Step 9: Clean up
                await self._close_session()

        except Exception as e:
            logger.error(f"Async ETL process failed: {e}")
            raise
        finally:
            if self.engine:
                self.engine.dispose()


async def main():
    """Main async function to run the ETL process"""
    try:
        # Load configuration to get default years
        with open("config.json", "r") as f:
            config = json.load(f)

        # Get default years from config
        default_years = config.get("etl", {}).get("census_years", [2015, 2019])
        begin_year = default_years[0]
        end_year = default_years[1]

        # Initialize ETL process with option to drop existing tables
        # Set drop_existing_tables=True to start fresh, False to keep existing data
        etl = AsyncCensusDataETL(drop_existing_tables=True)

        # Run ETL process with years from config
        await etl.run_etl_async(begin_year=begin_year, end_year=end_year)

        print("Async ETL process completed successfully!")

    except Exception as e:
        logger.error(f"Async ETL process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
