#!/usr/bin/env python3
"""
Modular ETL Controller
======================

This script serves as the main controller for the ETL system that orchestrates
data pulls from multiple sources:
- US Census Bureau API (via census_to_postgresql_async.py)
- Urban Institute API (via urban_to_postgresql_async.py)

Features:
- Centralized year control
- Modular design with separate components
- PostgreSQL database integration
- AWS integration
- Comprehensive error handling
- Rate limiting and retry logic
- Configurable data sources
- QA breakpoints for testing

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
import json
import os
import argparse
import asyncio
from typing import Any

# Import modular components
from census_to_postgresql_async import AsyncCensusDataETL
from urban_to_postgresql_async import AsyncUrbanDataETL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("main_etl.log"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# QA Configuration
QA_MODE = os.getenv("QA_MODE", "false").lower() == "true"
QA_BREAKPOINTS = os.getenv("QA_BREAKPOINTS", "false").lower() == "true"


def qa_breakpoint(message: str, data: Any = None):
    """QA breakpoint function for testing"""
    if QA_BREAKPOINTS:
        logger.info(f"🔍 QA BREAKPOINT: {message}")
        if data is not None:
            logger.info(f"📊 Data shape: {getattr(data, 'shape', 'N/A')}")
            logger.info(f"📋 Data columns: {getattr(data, 'columns', 'N/A')}")
        if QA_MODE:
            import pdb

            pdb.set_trace()
        else:
            input(f"Press Enter to continue... (QA: {message})")


class ModularETLController:
    """Main ETL controller that orchestrates multiple data sources"""

    def __init__(self, config_file="config.json"):
        """Initialize the modular ETL controller"""
        self.config = self._load_config(config_file)
        self.census_etl = None
        self.urban_etl = None
        self._initialize_etl_components()

        # Default year ranges from config
        self.census_years = self.config.get("etl", {}).get("census_years", [2015, 2019])
        self.urban_years = self.config.get("etl", {}).get("urban_years", [2020, 2023])

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

    def _initialize_etl_components(self):
        """Initialize ETL components for different data sources"""
        try:
            # Initialize Census ETL component
            self.census_etl = AsyncCensusDataETL(
                config_file="config.json", drop_existing_tables=True
            )
            logger.info("Census ETL component initialized")

            # Initialize Urban Institute ETL component
            self.urban_etl = AsyncUrbanDataETL(
                config_file="config.json", drop_existing_tables=True
            )
            logger.info("Urban Institute ETL component initialized")

        except Exception as e:
            logger.error(f"Failed to initialize ETL components: {e}")
            raise

    async def run_census_etl(self, begin_year: int = None, end_year: int = None):
        """Run Census ETL process"""
        try:
            # Use provided years or default from config
            if begin_year is None:
                begin_year = self.census_years[0]
            if end_year is None:
                end_year = self.census_years[1]

            logger.info(
                f"Starting Census ETL process for years {begin_year}-{end_year}"
            )

            qa_breakpoint("Starting Census ETL process", None)

            # Run Census ETL using the modular component
            await self.census_etl.run_etl_async(
                begin_year=begin_year, end_year=end_year
            )

            logger.info("Census ETL process completed successfully")

        except Exception as e:
            logger.error(f"Census ETL process failed: {e}")
            raise

    async def run_urban_etl(self, endpoints: list = None):
        """Run Urban Institute ETL process"""
        try:
            logger.info("Starting Urban Institute ETL process")

            qa_breakpoint("Starting Urban Institute ETL process", None)

            # Run Urban Institute ETL using the modular component
            await self.urban_etl.run_etl_async(endpoints=endpoints)

            logger.info("Urban Institute ETL process completed successfully")

        except Exception as e:
            logger.error(f"Urban Institute ETL process failed: {e}")
            raise

    async def run_multi_source_etl(
        self,
        census_begin_year: int = None,
        census_end_year: int = None,
        urban_endpoints: list = None,
    ):
        """Run complete multi-source ETL process"""
        try:
            logger.info("Starting multi-source ETL process")

            qa_breakpoint("Starting multi-source ETL process", None)

            # Run both ETL processes concurrently
            tasks = []

            # Add Census ETL task
            if census_begin_year is not None or census_end_year is not None:
                tasks.append(self.run_census_etl(census_begin_year, census_end_year))

            # Add Urban Institute ETL task
            if urban_endpoints is not None:
                tasks.append(self.run_urban_etl(urban_endpoints))

            # If no specific tasks, run with defaults
            if not tasks:
                tasks = [self.run_census_etl(), self.run_urban_etl()]

            # Execute all ETL processes concurrently
            await asyncio.gather(*tasks, return_exceptions=True)

            logger.info("Multi-source ETL process completed successfully")

        except Exception as e:
            logger.error(f"Multi-source ETL process failed: {e}")
            raise

    def get_etl_status(self):
        """Get status of ETL components"""
        status = {
            "census_etl": "initialized" if self.census_etl else "not_initialized",
            "urban_etl": "initialized" if self.urban_etl else "not_initialized",
            "config_years": {"census": self.census_years, "urban": self.urban_years},
        }
        return status


async def main():
    """Main async function to run the modular ETL process"""
    parser = argparse.ArgumentParser(
        description="Modular ETL Controller for Multi-Source Data"
    )
    parser.add_argument(
        "--census-begin-year", type=int, help="Start year for Census data fetch"
    )
    parser.add_argument(
        "--census-end-year", type=int, help="End year for Census data fetch"
    )
    parser.add_argument(
        "--urban-endpoints", nargs="+", help="Urban Institute endpoints to fetch"
    )
    parser.add_argument(
        "--config", type=str, default="config.json", help="Configuration file path"
    )
    parser.add_argument(
        "--qa-mode", action="store_true", help="Enable QA mode with breakpoints"
    )
    parser.add_argument(
        "--qa-breakpoints", action="store_true", help="Enable QA breakpoints"
    )
    parser.add_argument(
        "--status", action="store_true", help="Show ETL component status"
    )
    parser.add_argument(
        "--census-only", action="store_true", help="Run only Census ETL"
    )
    parser.add_argument(
        "--urban-only", action="store_true", help="Run only Urban Institute ETL"
    )

    args = parser.parse_args()

    # Set QA environment variables if specified
    if args.qa_mode:
        os.environ["QA_MODE"] = "true"
    if args.qa_breakpoints:
        os.environ["QA_BREAKPOINTS"] = "true"

    try:
        # Initialize ETL controller
        etl_controller = ModularETLController(args.config)

        # Show status if requested
        if args.status:
            status = etl_controller.get_etl_status()
            print("📊 ETL Component Status:")
            print(f"   Census ETL: {status['census_etl']}")
            print(f"   Urban ETL: {status['urban_etl']}")
            print(f"   Census years: {status['config_years']['census']}")
            print(f"   Urban years: {status['config_years']['urban']}")
            return

        # Run specific ETL processes based on arguments
        if args.census_only:
            await etl_controller.run_census_etl(
                args.census_begin_year, args.census_end_year
            )
        elif args.urban_only:
            await etl_controller.run_urban_etl(args.urban_endpoints)
        else:
            # Run multi-source ETL
            await etl_controller.run_multi_source_etl(
                census_begin_year=args.census_begin_year,
                census_end_year=args.census_end_year,
                urban_endpoints=args.urban_endpoints,
            )

        print("✅ Modular ETL process completed successfully!")

    except Exception as e:
        logger.error(f"Modular ETL process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
