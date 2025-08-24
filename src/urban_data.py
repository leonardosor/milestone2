#!/usr/bin/env python3
"""
Urban Institute Data ETL and Column Splitter
===========================================

This script provides both ETL functionality to fetch data from Urban Institute API
and splitting functionality to convert JSONB columns into separate tables.

Features:
- Fetch data from Urban Institute API endpoints
- Create base urban_institute_data table
- Split JSONB data into separate directory and endpoint tables
- Async processing for better performance

Requirements:
- psycopg2-binary
- pandas
- sqlalchemy
- aiohttp
- asyncio
"""

import json
import logging
import pandas as pd
import asyncio
import aiohttp
import argparse
from sqlalchemy import create_engine, text
from typing import Dict, Any, List, Set
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class AsyncUrbanDataETL:
    """Async ETL class for fetching Urban Institute data"""

    def __init__(self, config_file="config.json", drop_existing_tables=False):
        """Initialize the Urban Institute ETL process"""
        self.config = self._load_config(config_file)
        self.engine = None
        self.session = None
        self.drop_existing_tables = drop_existing_tables

    def _load_config(self, config_file):
        """Load configuration from JSON file"""
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
            logger.info("Configuration loaded successfully")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file {config_file} not found")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise

    def connect_to_database(self):
        """Establish connection to PostgreSQL database"""
        try:
            db_creds = self.config.get("local_database", {})

            connection_string = (
                f"postgresql://{db_creds['username']}:{db_creds['password']}"
                f"@{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
            )

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

            logger.info("Database connection established successfully")

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def create_tables(self):
        """Create the urban_institute_data table"""
        try:
            with self.engine.connect() as conn:
                if self.drop_existing_tables:
                    # Drop existing table if it exists
                    conn.execute(
                        text("DROP TABLE IF EXISTS urban_institute_data CASCADE;")
                    )
                    logger.info("Dropped existing urban_institute_data table")

                # Create the main table as documented
                create_sql = """
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

                -- Create indexes
                CREATE INDEX IF NOT EXISTS idx_urban_data_source ON urban_institute_data(data_source);
                CREATE INDEX IF NOT EXISTS idx_urban_data_endpoint ON urban_institute_data(endpoint);
                CREATE INDEX IF NOT EXISTS idx_urban_data_year ON urban_institute_data(year);
                CREATE INDEX IF NOT EXISTS idx_urban_data_json ON urban_institute_data USING GIN(data_json);
                """

                conn.execute(text(create_sql))
                conn.commit()
                logger.info("Successfully created urban_institute_data table")

        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    async def fetch_data_from_api(self, endpoint_template, year, params=None):
        """Fetch data from Urban Institute API"""
        if params is None:
            params = {}

        base_url = self.config.get("urban", {}).get("base_url", "")
        endpoint = endpoint_template.format(year=year)
        url = f"{base_url}{endpoint}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(
                            f"Successfully fetched data from {endpoint} for year {year}"
                        )
                        return data
                    else:
                        logger.error(
                            f"API request failed for {endpoint}: {response.status}"
                        )
                        return None
        except Exception as e:
            logger.error(f"Error fetching data from {endpoint}: {e}")
            return None

    async def insert_data_batch(self, data_batch):
        """Insert a batch of data into the database"""
        try:
            if not data_batch:
                return

            df = pd.DataFrame(data_batch)
            with self.engine.connect() as conn:
                df.to_sql(
                    "urban_institute_data",
                    conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                conn.commit()
                logger.info(f"Inserted batch of {len(data_batch)} records")

        except Exception as e:
            logger.error(f"Failed to insert data batch: {e}")
            raise

    async def run_etl_async(self, begin_year=None, end_year=None, endpoints=None):
        """Run the complete ETL process"""
        start_time = datetime.now()

        try:
            logger.info("=" * 80)
            logger.info("STARTING URBAN INSTITUTE ETL PROCESS")
            logger.info("=" * 80)

            # Step 1: Connect to database
            logger.info("Step 1/5: Connecting to database...")
            self.connect_to_database()

            # Step 2: Create tables
            logger.info("Step 2/5: Creating tables...")
            self.create_tables()

            # Step 3: Fetch data from API
            logger.info("Step 3/5: Fetching data from Urban Institute API...")

            # Use default years if not provided
            if begin_year is None:
                begin_year = 2020
            if end_year is None:
                end_year = 2023

            # Use default endpoints if not provided
            if endpoints is None:
                endpoints_config = self.config.get("urban", {}).get("endpoints", {})
                endpoints = list(endpoints_config.values())

            data_batch = []
            total_records = 0

            for year in range(begin_year, end_year + 1):
                for endpoint_template in endpoints:
                    logger.info(f"Fetching data for {endpoint_template} - year {year}")

                    data = await self.fetch_data_from_api(endpoint_template, year)
                    if data and "results" in data:
                        for record in data["results"]:
                            data_batch.append(
                                {
                                    "data_source": "urban_institute",
                                    "endpoint": endpoint_template,
                                    "year": year,
                                    "data_json": json.dumps(record),
                                    "fetched_at": datetime.now(),
                                    "created_at": datetime.now(),
                                    "updated_at": datetime.now(),
                                }
                            )

                        total_records += len(data["results"])

                        # Insert in batches of 1000
                        if len(data_batch) >= 1000:
                            await self.insert_data_batch(data_batch)
                            data_batch = []

            # Insert remaining records
            if data_batch:
                await self.insert_data_batch(data_batch)

            logger.info("Step 4/5: Running data splitter...")
            # Now run the splitter to create separate tables
            splitter = UrbanDataSplitter()
            splitter.config = self.config  # Share the config
            splitter.engine = self.engine  # Reuse the connection
            splitter.run_split_process("urban_data_expanded", begin_year, end_year)

            logger.info("Step 5/5: ETL process completed")

            end_time = datetime.now()
            duration = end_time - start_time

            logger.info("=" * 80)
            logger.info("URBAN INSTITUTE ETL COMPLETED SUCCESSFULLY!")
            logger.info(f"Total duration: {duration}")
            logger.info(f"Total records processed: {total_records}")
            logger.info(
                "Created tables: urban_institute_data, urban_data_expanded, urban_data_directory"
            )
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise
        finally:
            if self.engine:
                self.engine.dispose()


class UrbanDataSplitter:
    """Class to split JSONB data_json column into separate columns"""

    def __init__(self, config_file="config.json"):
        """Initialize with database configuration"""
        self.config = self._load_config(config_file)
        self.engine = None

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
            return {
                "local_database": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "milestone2",
                    "username": "postgres",
                    "password": "password",
                }
            }

    def connect_to_database(self):
        """Establish connection to PostgreSQL database"""
        try:
            db_creds = self.config.get("local_database", {})

            connection_string = (
                f"postgresql://{db_creds['username']}:{db_creds['password']}"
                f"@{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
            )

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

            logger.info("Database connection established successfully")

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def _sanitize_column_name(self, key: str) -> str:
        """Sanitize arbitrary JSON key into a safe SQL column name"""
        try:
            name = str(key)
            # Replace common problematic characters
            for ch in ["-", " ", ".", "(", ")", "/", "\\"]:
                name = name.replace(ch, "_")
            name = name.lower()
            # If starts with digit, prefix
            if name and name[0].isdigit():
                name = f"col_{name}"
            # Collapse consecutive underscores
            while "__" in name:
                name = name.replace("__", "_")
            return name
        except Exception:
            return "col_invalid"

    def analyze_json_structure(
        self, begin_year=None, end_year=None
    ) -> Dict[str, Set[str]]:
        """Analyze the structure of JSON data in data_json column to understand all possible keys"""
        logger.info("Analyzing JSON structure in data_json column...")

        try:
            with self.engine.connect() as conn:
                # Build year filter
                year_filter = ""
                if begin_year is not None and end_year is not None:
                    year_filter = f" AND year BETWEEN {begin_year} AND {end_year}"

                # Get sample of data_json to analyze structure
                query = f"""
                SELECT endpoint, year, data_json
                FROM urban_institute_data
                WHERE data_json IS NOT NULL{year_filter}
                LIMIT 1000
                """

                df = pd.read_sql(query, conn)

            if df.empty:
                logger.warning(
                    "No data found in urban_data table for the specified criteria"
                )
                return {}

            # Analyze JSON structure by endpoint
            structure = {}
            for _, row in df.iterrows():
                endpoint = row["endpoint"]
                if endpoint not in structure:
                    structure[endpoint] = set()

                try:
                    # Handle both string and already-parsed JSON
                    if isinstance(row["data_json"], str):
                        json_data = json.loads(row["data_json"])
                    else:
                        json_data = row["data_json"]

                    if isinstance(json_data, dict):
                        structure[endpoint].update(json_data.keys())
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(
                        f"Could not parse data_json for endpoint {endpoint}: {e}"
                    )
                    continue

            # Log structure analysis
            logger.info("Raw Data JSON Structure Analysis:")
            for endpoint, keys in structure.items():
                logger.info(f"  Endpoint: {endpoint}")
                logger.info(f"    Keys: {', '.join(sorted(keys))}")
                logger.info(f"    Total keys: {len(keys)}")

            return structure

        except Exception as e:
            logger.error(f"Failed to analyze data_json JSON structure: {e}")
            raise

    def get_all_unique_keys(
        self,
        begin_year=None,
        end_year=None,
        exclude_directory=False,
        directory_only=False,
    ) -> Set[str]:
        """Get all unique keys from all JSON data in data_json column"""
        logger.info("Extracting all unique keys from data_json column...")

        try:
            with self.engine.connect() as conn:
                # Build year filter if specified
                year_filter = ""
                if begin_year is not None and end_year is not None:
                    year_filter = f" AND year BETWEEN {begin_year} AND {end_year}"
                    logger.info(f"Analyzing keys for years {begin_year}-{end_year}")

                # Build endpoint filter
                endpoint_filter = ""
                if exclude_directory:
                    endpoint_filter = " AND endpoint NOT LIKE '%directory%'"
                    logger.info("Excluding directory endpoints")
                elif directory_only:
                    endpoint_filter = " AND endpoint LIKE '%directory%'"
                    logger.info("Including only directory endpoints")

                # Use PostgreSQL JSONB functions to get all keys from data_json
                query = f"""
                SELECT DISTINCT jsonb_object_keys(data_json::jsonb) as json_key
                FROM urban_institute_data
                WHERE data_json IS NOT NULL{year_filter}{endpoint_filter}
                ORDER BY json_key
                """

                result = conn.execute(text(query))
                keys = {row[0] for row in result.fetchall()}

            filter_desc = ""
            if exclude_directory:
                filter_desc = " (excluding directory)"
            elif directory_only:
                filter_desc = " (directory only)"

            logger.info(
                f"Found {len(keys)} unique keys from data_json column{filter_desc}: {', '.join(sorted(keys))}"
            )
            return keys

        except Exception as e:
            logger.error(f"Failed to get unique keys from data_json: {e}")
            raise

    def create_expanded_table(
        self,
        table_name="urban_data_expanded",
        begin_year=None,
        end_year=None,
        exclude_directory=False,
    ):
        """Create expanded table with separate columns for JSON fields"""
        logger.info(f"Creating expanded table: {table_name}")

        try:
            # Get all unique keys first
            unique_keys = self.get_all_unique_keys(
                begin_year, end_year, exclude_directory=exclude_directory
            )

            with self.engine.connect() as conn:
                # Drop existing expanded table if it exists
                drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
                conn.execute(text(drop_sql))
                conn.commit()

                # Create column definitions for each JSON key
                column_definitions = []

                # Standard columns from original table
                standard_columns = [
                    "id SERIAL PRIMARY KEY",
                    "data_source VARCHAR(50)",
                    "endpoint VARCHAR(255)",
                    "year INTEGER",
                    "fetched_at TIMESTAMP",
                    "created_at TIMESTAMP",
                ]

                # JSON field columns - using TEXT for flexibility
                # Exclude columns that already exist in standard columns
                standard_column_names = {
                    "id",
                    "data_source",
                    "endpoint",
                    "year",
                    "fetched_at",
                    "created_at",
                }

                json_columns = []
                for key in sorted(unique_keys):
                    # Sanitize column name consistently
                    clean_key = self._sanitize_column_name(key)
                    # Skip if this column already exists in standard columns
                    if clean_key.lower() not in standard_column_names:
                        json_columns.append(f"{clean_key} TEXT")

                all_columns = standard_columns + json_columns

                create_sql = f"""
                CREATE TABLE {table_name} (
                    {','.join(all_columns)}
                );

                -- Create indexes
                CREATE INDEX idx_{table_name}_year ON {table_name}(year);
                CREATE INDEX idx_{table_name}_endpoint ON {table_name}(endpoint);
                CREATE INDEX idx_{table_name}_data_source ON {table_name}(data_source);
                """

                conn.execute(text(create_sql))
                conn.commit()

            logger.info(
                f"Successfully created {table_name} with {len(json_columns)} JSON-derived columns"
            )

        except Exception as e:
            logger.error(f"Failed to create expanded table: {e}")
            raise

    def create_directory_table(
        self, table_name="urban_data_directory", begin_year=None, end_year=None
    ):
        """Create directory table specifically for directory endpoint data"""
        logger.info(f"Creating directory table: {table_name}")

        try:
            # Get unique keys only from directory endpoints
            unique_keys = self.get_all_unique_keys(
                begin_year, end_year, directory_only=True
            )

            with self.engine.connect() as conn:
                # Drop existing directory table if it exists
                drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
                conn.execute(text(drop_sql))
                conn.commit()

                # Standard columns from original table
                standard_columns = [
                    "id SERIAL PRIMARY KEY",
                    "data_source VARCHAR(50)",
                    "endpoint VARCHAR(255)",
                    "year INTEGER",
                    "fetched_at TIMESTAMP",
                    "created_at TIMESTAMP",
                ]

                # JSON field columns - using TEXT for flexibility
                # Exclude columns that already exist in standard columns
                standard_column_names = {
                    "id",
                    "data_source",
                    "endpoint",
                    "year",
                    "fetched_at",
                    "created_at",
                }

                json_columns = []
                for key in sorted(unique_keys):
                    # Sanitize column name consistently
                    clean_key = self._sanitize_column_name(key)
                    # Skip if this column already exists in standard columns
                    if clean_key.lower() not in standard_column_names:
                        json_columns.append(f"{clean_key} TEXT")

                all_columns = standard_columns + json_columns

                create_sql = f"""
                CREATE TABLE {table_name} (
                    {','.join(all_columns)}
                );

                -- Create indexes
                CREATE INDEX idx_{table_name}_year ON {table_name}(year);
                CREATE INDEX idx_{table_name}_endpoint ON {table_name}(endpoint);
                CREATE INDEX idx_{table_name}_data_source ON {table_name}(data_source);
                """

                conn.execute(text(create_sql))
                conn.commit()

            logger.info(
                f"Successfully created directory table {table_name} with {len(json_columns)} JSON-derived columns"
            )

        except Exception as e:
            logger.error(f"Failed to create directory table: {e}")
            raise

    def populate_expanded_table(
        self,
        table_name="urban_data_expanded",
        batch_size=1000,
        begin_year=None,
        end_year=None,
        exclude_directory=False,
    ):
        """Populate the expanded table with data from the original table"""
        year_filter = ""
        endpoint_filter = ""

        if begin_year is not None and end_year is not None:
            year_filter = f" WHERE year BETWEEN {begin_year} AND {end_year}"
            logger.info(
                f"Populating expanded table: {table_name} for years {begin_year}-{end_year}"
            )
        else:
            logger.info(f"Populating expanded table: {table_name} for all years")

        if exclude_directory:
            if year_filter:
                endpoint_filter = " AND endpoint NOT LIKE '%directory%'"
            else:
                year_filter = " WHERE endpoint NOT LIKE '%directory%'"
                endpoint_filter = ""
            logger.info("Excluding directory endpoints from expanded table")

        try:
            # Get unique keys for column mapping based on the same filters
            unique_keys = self.get_all_unique_keys(
                begin_year, end_year, exclude_directory=exclude_directory
            )

            with self.engine.connect() as conn:
                # Get total count for progress tracking
                count_query = f"SELECT COUNT(*) FROM urban_institute_data{year_filter}{endpoint_filter}"
                count_result = conn.execute(text(count_query))
                total_records = count_result.fetchone()[0]

                logger.info(f"Total records to process: {total_records}")

                processed = 0

                # Process in batches
                while processed < total_records:
                    logger.info(
                        f"Processing batch: {processed + 1} to {min(processed + batch_size, total_records)}"
                    )

                    # Fetch batch
                    base_query = """
                    SELECT id, data_source, endpoint, year, data_json, fetched_at, created_at
                    FROM urban_institute_data
                    """

                    full_filter = year_filter + endpoint_filter
                    if full_filter:
                        query = (
                            base_query
                            + full_filter
                            + f" ORDER BY id LIMIT {batch_size} OFFSET {processed}"
                        )
                    else:
                        query = (
                            base_query
                            + f" ORDER BY id LIMIT {batch_size} OFFSET {processed}"
                        )

                    logger.debug(f"Executing query: {query}")

                    # Execute the query directly using SQLAlchemy to avoid parameter issues
                    result = conn.execute(text(query))
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    logger.debug(f"Query returned {len(df)} rows")

                    if df.empty:
                        break

                    # Prepare data for insertion
                    insert_data = []

                    for _, row in df.iterrows():
                        record = {
                            "data_source": row["data_source"],
                            "endpoint": row["endpoint"],
                            "year": row["year"],
                            "fetched_at": row["fetched_at"],
                            "created_at": row["created_at"],
                        }

                        # Parse JSON data from data_json column
                        try:
                            if isinstance(row["data_json"], str):
                                json_data = json.loads(row["data_json"])
                            else:
                                json_data = row["data_json"]

                            if isinstance(json_data, dict):
                                for key in unique_keys:
                                    clean_key = self._sanitize_column_name(key)
                                    record[clean_key] = (
                                        str(json_data.get(key, ""))
                                        if json_data.get(key) is not None
                                        else ""
                                    )
                            else:
                                # Handle non-dict JSON data
                                for key in unique_keys:
                                    clean_key = self._sanitize_column_name(key)
                                    record[clean_key] = ""

                        except (json.JSONDecodeError, TypeError) as e:
                            logger.warning(
                                f"Could not parse data_json JSON for record {row['id']}: {e}"
                            )
                            # Fill with empty strings for all JSON columns
                            for key in unique_keys:
                                clean_key = self._sanitize_column_name(key)
                                record[clean_key] = ""

                        insert_data.append(record)

                    # Insert batch using pandas to_sql
                    if insert_data:
                        insert_df = pd.DataFrame(insert_data)
                        insert_df.to_sql(
                            table_name,
                            conn,
                            if_exists="append",
                            index=False,
                            method="multi",
                        )
                    conn.commit()

                    processed += len(df)
                    logger.info(
                        f"Processed {processed}/{total_records} records ({processed/total_records*100:.1f}%)"
                    )

                logger.info(
                    f"Successfully populated {table_name} with {processed} records"
                )

        except Exception as e:
            logger.error(f"Failed to populate expanded table: {e}")
            raise

    def populate_directory_table(
        self,
        table_name="urban_data_directory",
        batch_size=1000,
        begin_year=None,
        end_year=None,
    ):
        """Populate the directory table with data from directory endpoints only"""
        year_filter = ""
        endpoint_filter = " WHERE endpoint LIKE '%directory%'"

        if begin_year is not None and end_year is not None:
            year_filter = f" AND year BETWEEN {begin_year} AND {end_year}"
            logger.info(
                f"Populating directory table: {table_name} for years {begin_year}-{end_year}"
            )
        else:
            logger.info(f"Populating directory table: {table_name} for all years")

        try:
            # Get unique keys for column mapping from directory endpoints only
            unique_keys = self.get_all_unique_keys(
                begin_year, end_year, directory_only=True
            )

            with self.engine.connect() as conn:
                # Get total count for progress tracking
                count_query = f"SELECT COUNT(*) FROM urban_institute_data{endpoint_filter}{year_filter}"
                count_result = conn.execute(text(count_query))
                total_records = count_result.fetchone()[0]

                logger.info(f"Total directory records to process: {total_records}")

                processed = 0

                # Process in batches
                while processed < total_records:
                    logger.info(
                        f"Processing directory batch: {processed + 1} to {min(processed + batch_size, total_records)}"
                    )

                    # Fetch batch
                    base_query = """
                    SELECT id, data_source, endpoint, year, data_json, fetched_at, created_at
                    FROM urban_institute_data
                    """

                    query = (
                        base_query
                        + endpoint_filter
                        + year_filter
                        + f" ORDER BY id LIMIT {batch_size} OFFSET {processed}"
                    )
                    result = conn.execute(text(query))
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())

                    if df.empty:
                        break

                    # Prepare data for insertion
                    insert_data = []

                    for _, row in df.iterrows():
                        record = {
                            "data_source": row["data_source"],
                            "endpoint": row["endpoint"],
                            "year": row["year"],
                            "fetched_at": row["fetched_at"],
                            "created_at": row["created_at"],
                        }

                        # Parse JSON data from data_json column
                        try:
                            if isinstance(row["data_json"], str):
                                json_data = json.loads(row["data_json"])
                            else:
                                json_data = row["data_json"]

                            if isinstance(json_data, dict):
                                for key in unique_keys:
                                    clean_key = self._sanitize_column_name(key)
                                    record[clean_key] = (
                                        str(json_data.get(key, ""))
                                        if json_data.get(key) is not None
                                        else ""
                                    )
                            else:
                                # Handle non-dict JSON data
                                for key in unique_keys:
                                    clean_key = self._sanitize_column_name(key)
                                    record[clean_key] = ""

                        except (json.JSONDecodeError, TypeError) as e:
                            logger.warning(
                                f"Could not parse data_json JSON for record {row['id']}: {e}"
                            )
                            # Fill with empty strings for all JSON columns
                            for key in unique_keys:
                                clean_key = self._sanitize_column_name(key)
                                record[clean_key] = ""

                        insert_data.append(record)

                    # Insert batch using pandas to_sql
                    if insert_data:
                        insert_df = pd.DataFrame(insert_data)
                        insert_df.to_sql(
                            table_name,
                            conn,
                            if_exists="append",
                            index=False,
                            method="multi",
                        )
                        conn.commit()

                    processed += len(df)
                    logger.info(
                        f"Processed {processed}/{total_records} directory records ({processed/total_records*100:.1f}%)"
                    )

                logger.info(
                    f"Successfully populated directory table {table_name} with {processed} records"
                )

        except Exception as e:
            logger.error(f"Failed to populate directory table: {e}")
            raise

    def verify_expanded_table(self, table_name="urban_data_expanded"):
        """Verify the expanded table was created correctly"""
        logger.info(f"Verifying expanded table: {table_name}")

        try:
            with self.engine.connect() as conn:
                # Get table info
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                record_count = count_result.fetchone()[0]

                # Get column info
                columns_result = conn.execute(
                    text(
                        f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """
                    )
                )
                columns = columns_result.fetchall()

                logger.info("=" * 60)
                logger.info(f"EXPANDED TABLE VERIFICATION: {table_name}")
                logger.info("=" * 60)
                logger.info(f"Total records: {record_count}")
                logger.info(f"Total columns: {len(columns)}")
                logger.info("\nColumn structure:")

                for col_name, col_type in columns[:10]:  # Show first 10 columns
                    logger.info(f"  {col_name}: {col_type}")

                if len(columns) > 10:
                    logger.info(f"  ... and {len(columns) - 10} more columns")

                # Show sample data
                sample_result = conn.execute(
                    text(f"SELECT * FROM {table_name} LIMIT 3")
                )
                sample_data = sample_result.fetchall()
                sample_columns = [col[0] for col in columns]

                logger.info(f"\nSample data (first 3 records):")
                for i, record in enumerate(sample_data):
                    logger.info(f"Record {i+1}:")
                    for j, (col_name, _) in enumerate(
                        columns[:5]
                    ):  # Show first 5 columns
                        logger.info(f"  {col_name}: {record[j]}")
                    if len(columns) > 5:
                        logger.info(f"  ... and {len(columns) - 5} more fields")

                logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Failed to verify expanded table: {e}")
            raise

    def create_analysis_views(
        self,
        table_name="urban_data_expanded",
        directory_table_name="urban_data_directory",
    ):
        """Create useful views for data analysis based on actual columns"""
        logger.info("Creating analysis views...")

        try:
            with self.engine.connect() as conn:
                # Get actual columns from the directory table
                dir_columns_result = conn.execute(
                    text(
                        f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{directory_table_name}'
                    ORDER BY ordinal_position
                """
                    )
                )
                dir_available_columns = {
                    row[0] for row in dir_columns_result.fetchall()
                }

                # Get actual columns from the expanded table
                exp_columns_result = conn.execute(
                    text(
                        f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """
                    )
                )
                exp_available_columns = {
                    row[0] for row in exp_columns_result.fetchall()
                }

                logger.info(
                    f"Available columns in {directory_table_name}: {len(dir_available_columns)} columns"
                )
                logger.info(
                    f"Available columns in {table_name}: {len(exp_available_columns)} columns"
                )

                # Define possible columns for each view
                directory_columns = {
                    "year": "year",
                    "leaid": "leaid",
                    "lea_name": "lea_name",
                    "ncessch": "ncessch",
                    "school_name": "school_name",
                    "phone": "phone",
                    "mail_addr": "mail_addr",
                    "mail_city": "mail_city",
                    "mail_state": "mail_state",
                    "mail_zip": "mail_zip",
                    "school_type": "school_type",
                    "charter": "charter",
                    "magnet": "magnet",
                    "title_i_status": "title_i_status",
                    "enrollment": "enrollment",
                }

                enrollment_columns = {
                    "year": "year",
                    "leaid": "leaid",
                    "lea_name": "lea_name",
                    "ncessch": "ncessch",
                    "school_name": "school_name",
                    "grade": "grade",
                    "race": "race",
                    "sex": "sex",
                    "enrollment": "enrollment",
                }

                test_columns = {
                    "year": "year",
                    "leaid": "leaid",
                    "lea_name": "lea_name",
                    "ncessch": "ncessch",
                    "school_name": "school_name",
                    "sat_test_takers": "sat_test_takers",
                    "act_test_takers": "act_test_takers",
                    "total_enrollment": "total_enrollment",
                }

                # Filter columns that actually exist
                def filter_existing_columns(column_map, available_columns):
                    return [
                        col for col in column_map.values() if col in available_columns
                    ]

                # Create schools directory view from directory table
                directory_cols = filter_existing_columns(
                    directory_columns, dir_available_columns
                )
                if directory_cols:
                    schools_view = f"""
                    CREATE OR REPLACE VIEW schools_directory_view AS
                    SELECT {', '.join(directory_cols)}
                    FROM {directory_table_name}
                    WHERE {'leaid' if 'leaid' in dir_available_columns else 'year'} IS NOT NULL;
                    """
                    conn.execute(text(schools_view))
                    logger.info(
                        f"Created schools_directory_view with {len(directory_cols)} columns from {directory_table_name}"
                    )
                else:
                    logger.warning(
                        "Skipped schools_directory_view - no matching columns found"
                    )

                # Create enrollment view from expanded table
                enrollment_cols = filter_existing_columns(
                    enrollment_columns, exp_available_columns
                )
                if enrollment_cols:
                    enrollment_view = f"""
                    CREATE OR REPLACE VIEW enrollment_view AS
                    SELECT {', '.join(enrollment_cols)}
                    FROM {table_name}
                    WHERE endpoint LIKE '%enrollment%'
                    AND {'enrollment' if 'enrollment' in exp_available_columns else 'year'} IS NOT NULL;
                    """
                    conn.execute(text(enrollment_view))
                    logger.info(
                        f"Created enrollment_view with {len(enrollment_cols)} columns from {table_name}"
                    )
                else:
                    logger.warning(
                        "Skipped enrollment_view - no matching columns found"
                    )

                # Create test participation view from expanded table
                test_cols = filter_existing_columns(test_columns, exp_available_columns)
                if test_cols:
                    test_view = f"""
                    CREATE OR REPLACE VIEW test_participation_view AS
                    SELECT {', '.join(test_cols)}
                    FROM {table_name}
                    WHERE endpoint LIKE '%participation%';
                    """
                    conn.execute(text(test_view))
                    logger.info(
                        f"Created test_participation_view with {len(test_cols)} columns from {table_name}"
                    )
                else:
                    logger.warning(
                        "Skipped test_participation_view - no matching columns found"
                    )

                conn.commit()
                logger.info("Analysis views creation completed")

        except Exception as e:
            logger.error(f"Failed to create analysis views: {e}")
            raise

    def run_split_process(
        self, table_name="urban_data_expanded", begin_year=None, end_year=None
    ):
        """Run the complete splitting process"""
        start_time = datetime.now()

        try:
            logger.info("=" * 80)
            logger.info("STARTING URBAN DATA JSONB COLUMN SPLITTING PROCESS")
            if begin_year is not None and end_year is not None:
                logger.info(f"Processing years: {begin_year} to {end_year}")
            else:
                logger.info("Processing all years in database")
            logger.info("=" * 80)

            # Step 1: Connect to database
            logger.info("Step 1/8: Connecting to database...")
            self.connect_to_database()

            # Step 2: Analyze JSON structure from data_json
            logger.info("Step 2/8: Analyzing data_json JSON structure...")
            self.analyze_json_structure(begin_year, end_year)

            # Step 3: Create expanded table for non-directory data
            logger.info("Step 3/8: Creating expanded table for non-directory data...")
            self.create_expanded_table(
                table_name, begin_year, end_year, exclude_directory=True
            )

            # Step 4: Create directory table for directory data
            directory_table_name = f"{table_name.replace('_expanded', '')}_directory"
            logger.info(
                f"Step 4/8: Creating directory table: {directory_table_name}..."
            )
            self.create_directory_table(directory_table_name, begin_year, end_year)

            # Step 5: Populate expanded table (non-directory data)
            logger.info(
                "Step 5/8: Populating expanded table with non-directory data..."
            )
            self.populate_expanded_table(
                table_name,
                begin_year=begin_year,
                end_year=end_year,
                exclude_directory=True,
            )

            # Step 6: Populate directory table
            logger.info(
                f"Step 6/8: Populating directory table: {directory_table_name}..."
            )
            self.populate_directory_table(
                directory_table_name, begin_year=begin_year, end_year=end_year
            )

            # Step 7: Verify results
            logger.info("Step 7/8: Verifying tables...")
            self.verify_expanded_table(table_name)
            self.verify_expanded_table(directory_table_name)

            # Step 8: Create analysis views
            logger.info("Step 8/8: Creating analysis views...")
            self.create_analysis_views(table_name, directory_table_name)

            end_time = datetime.now()
            duration = end_time - start_time

            logger.info("=" * 80)
            logger.info("JSONB SPLITTING PROCESS COMPLETED SUCCESSFULLY!")
            logger.info(f"Total duration: {duration}")
            logger.info(f"Expanded table created: {table_name}")
            logger.info(f"Directory table created: {directory_table_name}")
            if begin_year is not None and end_year is not None:
                logger.info(f"Years processed: {begin_year} to {end_year}")
            logger.info("Raw_data JSON keys successfully split into individual columns")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Splitting process failed: {e}")
            raise
        finally:
            if self.engine:
                self.engine.dispose()


async def main():
    """Main function to run ETL or splitting process"""
    parser = argparse.ArgumentParser(
        description="Urban Institute ETL and Data Splitting"
    )
    parser.add_argument(
        "--config", type=str, default="config.json", help="Configuration file path"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["etl", "split"],
        default="etl",
        help="Mode: 'etl' for full ETL process, 'split' for splitting only",
    )
    parser.add_argument(
        "--table-name",
        type=str,
        default="urban_data_expanded",
        help="Name for expanded table",
    )
    parser.add_argument("--begin-year", type=int, help="Start year for data processing")
    parser.add_argument("--end-year", type=int, help="End year for data processing")
    parser.add_argument(
        "--drop-tables",
        action="store_true",
        help="Drop existing tables before creating new ones",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate year parameters
    if (args.begin_year is not None and args.end_year is None) or (
        args.begin_year is None and args.end_year is not None
    ):
        logger.error(
            "Both --begin-year and --end-year must be specified together, or neither"
        )
        sys.exit(1)

    if (
        args.begin_year is not None
        and args.end_year is not None
        and args.begin_year > args.end_year
    ):
        logger.error("--begin-year cannot be greater than --end-year")
        sys.exit(1)

    try:
        if args.mode == "etl":
            # Run full ETL process (fetch data + create tables)
            logger.info("Running Urban Institute ETL process...")
            etl = AsyncUrbanDataETL(args.config, drop_existing_tables=args.drop_tables)
            await etl.run_etl_async(args.begin_year, args.end_year)

        elif args.mode == "split":
            # Run splitting process only
            logger.info("Running Urban Institute data splitting process...")
            splitter = UrbanDataSplitter(config_file=args.config)
            splitter.run_split_process(
                table_name=args.table_name,
                begin_year=args.begin_year,
                end_year=args.end_year,
            )

        logger.info("Process completed successfully!")

    except Exception as e:
        logger.error(f"Process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
