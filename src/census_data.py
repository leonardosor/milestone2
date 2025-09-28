#!/usr/bin/env python3
"""
Simplified Census Data ETL to PostgreSQL Database
================================================

This script provides simplified ETL functionality for Census API data.
Maintains logging and core functionality while being easier to debug.
"""

import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime

import censusdata
import pandas as pd
from sqlalchemy import create_engine, text

# Database schema configuration - read from config file
DB_SCHEMA = None  # Will be set from config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("../logs/census_etl_simple.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class SimpleCensusETL:
    """Simplified ETL class for Census data to PostgreSQL"""

    def __init__(self, config_file="config.json"):
        """Initialize the ETL process with configuration.

            If the provided config_file is relative and not found in the current
            working directory, we attempt to discover it by searching parent
            directories (up to 5 levels) relative to this script's location.
        This has been simplified: we no longer use an environment variable
        override; we just look upward for the first matching config filename.
        """
        resolved = self._resolve_config_path(config_file)
        if resolved != config_file:
            logger.info(f"🔍 Resolved config file: {resolved}")
        else:
            logger.info(f"🔍 Using config file: {resolved}")
        self.config = self._load_config(resolved)
        self.engine = None

    def _resolve_config_path(self, path: str) -> str:
        # Absolute path supplied and exists
        if os.path.isabs(path) and os.path.isfile(path):
            return path
        # If relative and exists in CWD, use it
        if not os.path.isabs(path) and os.path.isfile(path):
            return os.path.abspath(path)
        # Search upward from script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = []
        for depth in range(0, 6):
            candidate = os.path.join(script_dir, *([".."] * depth), path)
            candidate = os.path.abspath(candidate)
            if candidate not in candidates:
                candidates.append(candidate)
            if os.path.isfile(candidate):
                return candidate
        logger.debug("Config file not found in candidates: " + "; ".join(candidates))
        return path  # Let _load_config handle fallback

    def _load_config(self, config_file):
        """Load configuration from JSON file or use defaults"""
        global DB_SCHEMA
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
            logger.info(f"✅ Configuration loaded: {config_file}")
            # Set global schema from config
            DB_SCHEMA = config.get("schema", "public")
            return config
        except FileNotFoundError:
            logger.warning(
                f"⚠️ Configuration file {config_file} not found. Using built-in defaults."
            )
            default_config = {
                "local_database": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "milestone2",
                    "username": "postgres",
                    "password": "123",
                },
                "schema": "public",
            }
            DB_SCHEMA = default_config.get("schema", "public")
            return default_config
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise

    def connect_to_database(self):
        """Establish connection to PostgreSQL database"""
        try:
            # Always use local database for simplicity
            db_creds = self.config.get("local_database", {})

            if not db_creds or not all(
                key in db_creds
                for key in ["host", "port", "database", "username", "password"]
            ):
                logger.error("Incomplete local database configuration")
                raise ValueError(
                    "Local database configuration is incomplete. Please check config.json"
                )

            connection_string = (
                f"postgresql://{db_creds['username']}:{db_creds['password']}"
                f"@{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
            )

            self.engine = create_engine(connection_string)

            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            # Ensure target schema exists
            if DB_SCHEMA:
                try:
                    with self.engine.connect() as conn:
                        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA};"))
                        conn.commit()
                    logger.info(f"✅ Schema '{DB_SCHEMA}' is ready")
                except Exception as sce:
                    logger.error(f"❌ Failed ensuring schema '{DB_SCHEMA}': {sce}")
                    raise

            logger.info("✅ Database connection established successfully")

        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            logger.error(
                f"Connection string template: postgresql://username:password@host:port/database"
            )
            logger.error(
                f"Check that PostgreSQL is running and credentials are correct"
            )
            raise

    def create_tables(self):
        """Create database tables"""
        try:
            with self.engine.connect() as conn:
                logger.info("🗄️ Creating database tables...")

                # Drop existing table
                drop_sql = f"DROP TABLE IF EXISTS {DB_SCHEMA}.census_data CASCADE;"
                conn.execute(text(drop_sql))

                # Create new table
                create_sql = f"""
                CREATE TABLE {DB_SCHEMA}.census_data (
                    id SERIAL PRIMARY KEY,
                    zip_code VARCHAR(10),
                    year INTEGER,
                    total_pop INTEGER,
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
                    data_source VARCHAR(50) DEFAULT 'census_api',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                conn.execute(text(create_sql))

                # Create indexes
                conn.execute(
                    text(
                        f"CREATE INDEX idx_census_zip_year ON {DB_SCHEMA}.census_data(zip_code, year);"
                    )
                )
                conn.execute(
                    text(
                        f"CREATE INDEX idx_census_year ON {DB_SCHEMA}.census_data(year);"
                    )
                )

                conn.commit()
                logger.info("✅ Database tables created successfully")

        except Exception as e:
            logger.error(f"❌ Failed to create tables: {e}")
            raise

    def fetch_census_data(self, year):
        """Fetch census data for a single year"""
        try:
            logger.info(f"📥 Fetching Census data for year {year}...")

            # Define census variables
            census_variables = {
                "B01003_001E": "total_pop",
                "B19001_016E": "hhi_150k_200k",
                "B19001_017E": "hhi_220k_plus",
                "B01001_006E": "males_15_17",
                "B01001_030E": "females_15_17",
                "B01001A_006E": "white_males_15_17",
                "B01001B_006E": "black_males_15_17",
                "B01001I_006E": "hispanic_males_15_17",
                "B01001A_021E": "white_females_15_17",
                "B01001B_021E": "black_females_15_17",
                "B01001I_021E": "hispanic_females_15_17",
            }

            variable_codes = list(census_variables.keys())

            # Fetch data from Census API
            census_data = censusdata.download(
                "acs5",
                year,
                censusdata.censusgeo([("zip code tabulation area", "*")]),
                variable_codes,
            )

            if census_data.empty:
                logger.warning(f"⚠️ No data returned for year {year}")
                return pd.DataFrame()

            # Clean up the data
            census_data.reset_index(inplace=True)

            # Extract zip codes from CensusGeo objects
            census_data["zip_code"] = census_data["index"].apply(
                lambda x: x.params()[0][1]
                if hasattr(x, "params") and x.params()
                else str(x)
            )

            # Rename columns to simplified names
            for old_name, new_name in census_variables.items():
                if old_name in census_data.columns:
                    census_data.rename(columns={old_name: new_name}, inplace=True)

            # Add metadata
            census_data["year"] = year
            census_data["data_source"] = "census_api"

            # Drop the index column
            census_data.drop(columns=["index"], inplace=True, errors="ignore")

            # Fill NaN values with 0
            for col in census_data.columns:
                if col not in ["zip_code", "year", "data_source"]:
                    census_data[col] = census_data[col].fillna(0)

            logger.info(
                f"✅ Successfully fetched {len(census_data)} records for year {year}"
            )
            return census_data

        except Exception as e:
            logger.error(f"❌ Failed to fetch census data for year {year}: {e}")
            logger.error(f"Full error: {traceback.format_exc()}")
            return pd.DataFrame()

    def insert_data_to_db(self, data):
        """Insert data into database"""
        try:
            if data.empty:
                logger.warning("⚠️ No data to insert")
                return 0

            logger.info(f"💾 Inserting {len(data)} records into database...")

            # Use pandas to_sql for simplicity
            # Insert records (return value unused by pandas; flake8 F841 fix)
            data.to_sql(
                "census_data",
                self.engine,
                schema=DB_SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
            )

            logger.info(f"✅ Successfully inserted {len(data)} records")
            return len(data)

        except Exception as e:
            logger.error(f"❌ Failed to insert data: {e}")
            logger.error(f"Full error: {traceback.format_exc()}")
            return 0

    def save_to_csv(self, data, filename):
        """Save data to CSV file"""
        try:
            if data.empty:
                logger.warning("⚠️ No data to save to CSV")
                return

            data.to_csv(f"../outputs/{filename}", index=False, encoding="utf-8")
            logger.info(f"✅ Data saved to ./outputs/{filename}")

        except Exception as e:
            logger.error(f"❌ Failed to save CSV: {e}")

    def run_etl(self, begin_year, end_year):
        """Run the complete ETL process"""
        start_time = datetime.now()
        total_years = end_year - begin_year + 1
        total_inserted = 0
        all_data = []

        try:
            logger.info("=" * 60)
            logger.info(f"🚀 STARTING SIMPLIFIED CENSUS ETL PROCESS")
            logger.info(
                f"📅 Processing years: {begin_year} to {end_year} ({total_years} years)"
            )
            logger.info(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)

            # Step 1: Connect to database
            logger.info("🔌 Step 1/4: Connecting to database...")
            self.connect_to_database()

            # Step 2: Create tables
            logger.info("🏗️ Step 2/4: Creating database tables...")
            self.create_tables()

            # Step 3: Fetch and insert data for each year
            logger.info("📥 Step 3/4: Fetching and inserting Census data...")
            for i, year in enumerate(range(begin_year, end_year + 1)):
                progress = (i + 1) / total_years * 100
                logger.info(
                    f"Processing year {year} ({i+1}/{total_years}) - {progress:.1f}%"
                )

                # Fetch data for this year
                year_data = self.fetch_census_data(year)

                if not year_data.empty:
                    # Insert into database
                    inserted = self.insert_data_to_db(year_data)
                    total_inserted += inserted
                    all_data.append(year_data)
                else:
                    logger.warning(f"⚠️ No data for year {year}")

            # Step 4: Save consolidated CSV
            logger.info("💾 Step 4/4: Saving consolidated CSV...")
            if all_data:
                consolidated_data = pd.concat(all_data, ignore_index=True)
                self.save_to_csv(consolidated_data, "census_data_consolidated.csv")

            # Final summary
            end_time = datetime.now()
            duration = end_time - start_time

            logger.info("=" * 60)
            logger.info(f"🎉 CENSUS ETL PROCESS COMPLETED SUCCESSFULLY!")
            logger.info(f"⏱️ Total duration: {duration}")
            logger.info(f"📊 Total records inserted: {total_inserted}")
            logger.info(f"📅 Years processed: {begin_year} to {end_year}")
            logger.info(f"🏁 Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)

        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time
            logger.error("=" * 60)
            logger.error(f"💥 CENSUS ETL PROCESS FAILED!")
            logger.error(f"⏱️ Duration before failure: {duration}")
            logger.error(f"❌ Error: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            logger.error("=" * 60)
            raise
        finally:
            if self.engine:
                self.engine.dispose()


def main():
    """Main function to run the ETL process"""
    parser = argparse.ArgumentParser(description="Simplified Census ETL Process")
    parser.add_argument(
        "--begin-year", type=int, required=True, help="Start year for Census data fetch"
    )
    parser.add_argument(
        "--end-year", type=int, required=True, help="End year for Census data fetch"
    )
    parser.add_argument(
        "--config", type=str, default="config.json", help="Configuration file path"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("🔍 Verbose logging enabled")

    try:
        logger.info("🚀 Initializing Simplified Census ETL process...")
        logger.info(f"📅 Year range: {args.begin_year} to {args.end_year}")
        logger.info(f"⚙️ Config file: {args.config}")

        etl = SimpleCensusETL(config_file=args.config)
        logger.info("✅ ETL process initialized successfully")

        logger.info("🔄 Starting ETL execution...")
        etl.run_etl(begin_year=args.begin_year, end_year=args.end_year)

        logger.info("🎉 Simplified Census ETL process completed successfully!")

    except Exception as e:
        logger.error(f"💥 Simplified Census ETL process failed: {e}")

        # Provide helpful suggestions
        if "connection" in str(e).lower() or "database" in str(e).lower():
            logger.error("\n" + "=" * 60)
            logger.error("🗄️ DATABASE CONNECTION ERROR")
            logger.error("=" * 60)
            logger.error("💡 Recommendations:")
            logger.error("1. Check if PostgreSQL is running")
            logger.error("2. Verify database credentials in config.json")
            logger.error("3. Ensure database 'milestone2' exists")
            logger.error("4. Check if user 'postgres' has proper permissions")
            logger.error("=" * 60)
        elif "census" in str(e).lower() or "api" in str(e).lower():
            logger.error("\n" + "=" * 60)
            logger.error("🌐 CENSUS API ERROR")
            logger.error("=" * 60)
            logger.error("💡 Recommendations:")
            logger.error("1. Check your internet connection")
            logger.error("2. Try a different year range")
            logger.error("3. The Census API may be temporarily down")
            logger.error("=" * 60)

        sys.exit(1)


if __name__ == "__main__":
    main()
