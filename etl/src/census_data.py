#!/usr/bin/env python3
"""Census API ETL"""

import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
import censusdata
import pandas as pd
from sqlalchemy import create_engine, text

# Import ConfigLoader
sys.path.append(str(Path(__file__).parent.parent / "config"))
from config_loader import ConfigLoader

DB_SCHEMA = None

# Ensure logs directory exists
os.makedirs("/app/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/census_etl_simple.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

class SimpleCensusETL:

    def __init__(self, config_file="config.json"):
        # Use new ConfigLoader
        self.config_loader = ConfigLoader(config_file)
        self.config = self.config_loader.config
        global DB_SCHEMA
        DB_SCHEMA = self.config.get("schema", "public")
        logger.info(f"Configuration loaded with schema: {DB_SCHEMA}")
        self.engine = None

    def connect_to_database(self):
        try:
            # Use ConfigLoader's connection string method
            connection_string = self.config_loader.get_db_connection_string()
            logger.info(f"Connecting to database...")

            self.engine = create_engine(connection_string, pool_pre_ping=True)

            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            if DB_SCHEMA:
                with self.engine.connect() as conn:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA};"))
                    conn.commit()
                logger.info(f"Schema '{DB_SCHEMA}' ready")

            logger.info("Database connected successfully")

        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    def create_tables(self):
        try:
            with self.engine.connect() as conn:
                logger.info("Creating tables...")
                drop_sql = f"DROP TABLE IF EXISTS {DB_SCHEMA}.census_data CASCADE;"
                conn.execute(text(drop_sql))
                create_sql = f"""
                CREATE TABLE {DB_SCHEMA}.census_data (
                    id SERIAL PRIMARY KEY,
                    zip_code VARCHAR(10),
                    year INTEGER,
                    total_pop INTEGER,
                    hhi_150k_200k INTEGER,
                    hhi_220k_plus INTEGER,
                    males_10_14 INTEGER,
                    females_10_14 INTEGER,
                    white_males_10_14 INTEGER,
                    black_males_10_14 INTEGER,
                    hispanic_males_10_14 INTEGER,
                    white_females_10_14 INTEGER,
                    black_females_10_14 INTEGER,
                    hispanic_females_10_14 INTEGER,
                    data_source VARCHAR(50) DEFAULT 'census_api',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                conn.execute(text(create_sql))
                conn.execute(text(f"CREATE INDEX idx_census_zip_year ON {DB_SCHEMA}.census_data(zip_code, year);"))
                conn.execute(text(f"CREATE INDEX idx_census_year ON {DB_SCHEMA}.census_data(year);"))
                conn.commit()
                logger.info("Tables created")

        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def fetch_census_data(self, year):
        try:
            logger.info(f"Fetching data for {year}...")

            census_variables = {
                "B01003_001E": "total_pop",
                "B19001_016E": "hhi_150k_200k",
                "B19001_017E": "hhi_220k_plus",
                "B01001_005E": "males_10_14",
                "B01001_029E": "females_10_14",
                "B01001A_005E": "white_males_10_14",
                "B01001B_005E": "black_males_10_14",
                "B01001I_005E": "hispanic_males_10_14",
                "B01001A_020E": "white_females_10_14",
                "B01001B_020E": "black_females_10_14",
                "B01001I_020E": "hispanic_females_10_14"
            }

            variable_codes = list(census_variables.keys())
            census_data = censusdata.download(
                "acs5",
                year,
                censusdata.censusgeo([("zip code tabulation area", "*")]),
                variable_codes,
            )

            if census_data.empty:
                logger.warning(f"No data for {year}")
                return pd.DataFrame()

            census_data.reset_index(inplace=True)
            census_data["zip_code"] = census_data["index"].apply(
                lambda x: x.params()[0][1]
                if hasattr(x, "params") and x.params()
                else str(x)
            )

            for old_name, new_name in census_variables.items():
                if old_name in census_data.columns:
                    census_data.rename(columns={old_name: new_name}, inplace=True)

            census_data["year"] = year
            census_data["data_source"] = "census_api"
            census_data.drop(columns=["index"], inplace=True, errors="ignore")
            for col in census_data.columns:
                if col not in ["zip_code", "year", "data_source"]:
                    census_data[col] = census_data[col].fillna(0)

            logger.info(f"Fetched {len(census_data)} records for {year}")
            return census_data

        except Exception as e:
            logger.error(f"Failed to fetch {year}: {e}")
            return pd.DataFrame()

    def insert_data_to_db(self, data):
        try:
            if data.empty:
                logger.warning("No data to insert")
                return 0

            logger.info(f"Inserting {len(data)} records...")
            data.to_sql(
                "census_data",
                self.engine,
                schema=DB_SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
            )

            logger.info(f"Inserted {len(data)} records")
            return len(data)

        except Exception as e:
            logger.error(f"Insert failed: {e}")
            return 0

    def save_to_csv(self, data, filename):
        try:
            if data.empty:
                logger.warning("No data to save")
                return

            data.to_csv(f"../outputs/{filename}", index=False, encoding="utf-8")
            logger.info(f"Saved to ./outputs/{filename}")

        except Exception as e:
            logger.error(f"CSV save failed: {e}")

    def run_etl(self, begin_year, end_year):
        start_time = datetime.now()
        total_years = end_year - begin_year + 1
        total_inserted = 0
        all_data = []

        try:
            logger.info("=" * 60)
            logger.info(f"CENSUS ETL: {begin_year} to {end_year} ({total_years} years)")
            logger.info(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)

            self.connect_to_database()
            self.create_tables()
            for i, year in enumerate(range(begin_year, end_year + 1)):
                progress = (i + 1) / total_years * 100
                logger.info(f"{year} ({i+1}/{total_years}) - {progress:.1f}%")

                year_data = self.fetch_census_data(year)
                if not year_data.empty:
                    inserted = self.insert_data_to_db(year_data)
                    total_inserted += inserted
                    all_data.append(year_data)
                else:
                    logger.warning(f"No data for {year}")
            if all_data:
                consolidated_data = pd.concat(all_data, ignore_index=True)
                self.save_to_csv(consolidated_data, "census_data_consolidated.csv")

            end_time = datetime.now()
            duration = end_time - start_time

            logger.info("=" * 60)
            logger.info(f"CENSUS ETL COMPLETED")
            logger.info(f"Duration: {duration}")
            logger.info(f"Records: {total_inserted}")
            logger.info(f"Years: {begin_year} to {end_year}")
            logger.info(f"Completed: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)

        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time
            logger.error("=" * 60)
            logger.error(f"CENSUS ETL FAILED")
            logger.error(f"Duration: {duration}")
            logger.error(f"Error: {e}")
            logger.error("=" * 60)
            raise
        finally:
            if self.engine:
                self.engine.dispose()


def main():
    parser = argparse.ArgumentParser(description="Census ETL Process")
    parser.add_argument("--begin-year", type=int, required=True, help="Start year")
    parser.add_argument("--end-year", type=int, required=True, help="End year")
    parser.add_argument("--config", type=str, default="config.json", help="Config file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        logger.info(f"Census ETL: {args.begin_year} to {args.end_year}")
        etl = SimpleCensusETL(config_file=args.config)
        etl.run_etl(begin_year=args.begin_year, end_year=args.end_year)
        logger.info("Census ETL completed")

    except Exception as e:
        logger.error(f"Census ETL failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()