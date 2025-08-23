#!/usr/bin/env python3
"""
Orchestrated ETL Controller
===========================

This script serves as the main orchestrator for the complete ETL system that runs
data processing from multiple sources in the correct sequence:
1. US Census Bureau API data collection (census_data.py)
2. Urban Institute API data collection (urban_data.py)  
3. Coordinate to zipcode geocoding (location_data.py)

Features:
- Sequential processing pipeline
- Centralized year control
- PostgreSQL database integration
- Comprehensive error handling
- Progress tracking and logging
- Configurable parameters for each stage

Requirements:
- All dependencies from individual scripts
- psycopg2-binary, pandas, sqlalchemy, aiohttp, requests, etc.
"""

import sys
import logging
import json
import os
import argparse
import asyncio
import subprocess
from typing import Any
from datetime import datetime

# Import modular components
from census_data import SimpleCensusETL
from urban_data import AsyncUrbanDataETL
from location_data import process_coordinates, test_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("./logs/main_etl.log"), logging.StreamHandler(sys.stdout)],
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


class OrchestatedETLController:
    """Main ETL controller that orchestrates the complete data pipeline"""

    def __init__(self, config_file="config.json"):
        """Initialize the orchestrated ETL controller"""
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
            self.census_etl = SimpleCensusETL(
                config_file="config.json"
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

    def run_census_etl(self, begin_year: int = None, end_year: int = None):
        """Run Census ETL process"""
        try:
            # Use provided years or default from config
            if begin_year is None:
                begin_year = self.census_years[0]
            if end_year is None:
                end_year = self.census_years[1]

            logger.info(f"🏛️ Starting Census ETL process for years {begin_year}-{end_year}")
            qa_breakpoint("Starting Census ETL process", None)

            # Run Census ETL using the modular component
            self.census_etl.run_etl(begin_year=begin_year, end_year=end_year)
            logger.info("✅ Census ETL process completed successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Census ETL process failed: {e}")
            raise

    async def run_urban_etl(self, begin_year: int = None, end_year: int = None, endpoints: list = None):
        """Run Urban Institute ETL process"""
        try:
            # Use config defaults if no years specified
            if begin_year is None:
                begin_year = self.urban_years[0]
            if end_year is None:
                end_year = self.urban_years[1]
                
            logger.info(f"🏙️ Starting Urban Institute ETL process for years {begin_year}-{end_year}")
            qa_breakpoint("Starting Urban Institute ETL process", None)

            # Run Urban Institute ETL using the modular component
            await self.urban_etl.run_etl_async(begin_year=begin_year, end_year=end_year, endpoints=endpoints)
            logger.info("✅ Urban Institute ETL process completed successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Urban Institute ETL process failed: {e}")
            raise

    def run_location_etl(self, batch_size: int = 50, max_coordinates: int = None, table_name: str = "lat_lon_zipcode"):
        """Run Location Data ETL process (coordinates to zipcodes)"""
        try:
            logger.info(f"📍 Starting Location Data ETL process")
            qa_breakpoint("Starting Location Data ETL process", None)

            # Test database connection first
            if not test_connection():
                raise Exception("Database connection failed for location ETL")

            # Run location data processing
            process_coordinates(
                batch_size=batch_size,
                max_coordinates=max_coordinates,
                table_name=table_name
            )
            logger.info("✅ Location Data ETL process completed successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Location Data ETL process failed: {e}")
            raise

    async def run_complete_pipeline(
        self,
        census_begin_year: int = None,
        census_end_year: int = None,
        urban_begin_year: int = None,
        urban_end_year: int = None,
        urban_endpoints: list = None,
        location_batch_size: int = 50,
        location_max_coords: int = None,
        location_table_name: str = "lat_lon_zipcode",
        skip_census: bool = False,
        skip_urban: bool = False,
        skip_location: bool = False,
    ):
        """Run the complete ETL pipeline in sequence"""
        try:
            pipeline_start = datetime.now()
            logger.info("🚀 Starting Complete ETL Pipeline")
            logger.info("=" * 60)

            qa_breakpoint("Starting complete ETL pipeline", None)

            # Stage 1: Census Data ETL
            if not skip_census:
                logger.info("📊 STAGE 1: Census Data Collection")
                logger.info("-" * 40)
                self.run_census_etl(census_begin_year, census_end_year)
                logger.info("✅ Stage 1 completed\n")
            else:
                logger.info("⏭️ Skipping Census ETL")

            # Stage 2: Urban Institute Data ETL  
            if not skip_urban:
                logger.info("📊 STAGE 2: Urban Institute Data Collection")
                logger.info("-" * 40)
                await self.run_urban_etl(urban_begin_year, urban_end_year, urban_endpoints)
                logger.info("✅ Stage 2 completed\n")
            else:
                logger.info("⏭️ Skipping Urban ETL")

            # Stage 3: Location Data ETL (coordinates to zipcodes)
            if not skip_location:
                logger.info("📊 STAGE 3: Location Data Processing (Coordinates → Zipcodes)")
                logger.info("-" * 40)
                self.run_location_etl(location_batch_size, location_max_coords, location_table_name)
                logger.info("✅ Stage 3 completed\n")
            else:
                logger.info("⏭️ Skipping Location ETL")

            # Pipeline completion
            pipeline_end = datetime.now()
            duration = pipeline_end - pipeline_start
            
            logger.info("=" * 60)
            logger.info("🎉 COMPLETE ETL PIPELINE FINISHED SUCCESSFULLY!")
            logger.info(f"⏱️ Total pipeline duration: {duration}")
            logger.info(f"🏁 Completed at: {pipeline_end.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"❌ Complete ETL pipeline failed: {e}")
            raise

    def get_etl_status(self):
        """Get status of ETL components"""
        status = {
            "census_etl": "initialized" if self.census_etl else "not_initialized",
            "urban_etl": "initialized" if self.urban_etl else "not_initialized",
            "location_etl": "available",
            "config_years": {"census": self.census_years, "urban": self.urban_years},
        }
        return status


async def main():
    """Main async function to run the orchestrated ETL pipeline"""
    parser = argparse.ArgumentParser(
        description="Orchestrated ETL Controller for Complete Data Pipeline"
    )
    
    # Census arguments
    parser.add_argument(
        "--census-begin-year", type=int, help="Start year for Census data fetch"
    )
    parser.add_argument(
        "--census-end-year", type=int, help="End year for Census data fetch"
    )
    
    # Urban Institute arguments
    parser.add_argument(
        "--urban-begin-year", type=int, help="Start year for Urban Institute data fetch"
    )
    parser.add_argument(
        "--urban-end-year", type=int, help="End year for Urban Institute data fetch"
    )
    parser.add_argument(
        "--urban-endpoints", nargs="+", help="Urban Institute endpoints to fetch (optional)"
    )
    
    # Location data arguments
    parser.add_argument(
        "--location-batch-size", type=int, default=50, help="Batch size for location processing (default: 50)"
    )
    parser.add_argument(
        "--location-max-coords", type=int, help="Maximum coordinates to process (default: all)"
    )
    parser.add_argument(
        "--location-table-name", type=str, default="lat_lon_zipcode", 
        help="Name of location results table (default: lat_lon_zipcode)"
    )
    
    # Control arguments
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
    
    # Pipeline control
    parser.add_argument(
        "--census-only", action="store_true", help="Run only Census ETL"
    )
    parser.add_argument(
        "--urban-only", action="store_true", help="Run only Urban Institute ETL"
    )
    parser.add_argument(
        "--location-only", action="store_true", help="Run only Location Data ETL"
    )
    parser.add_argument(
        "--skip-census", action="store_true", help="Skip Census ETL in complete pipeline"
    )
    parser.add_argument(
        "--skip-urban", action="store_true", help="Skip Urban ETL in complete pipeline"
    )
    parser.add_argument(
        "--skip-location", action="store_true", help="Skip Location ETL in complete pipeline"
    )

    args = parser.parse_args()

    # Set QA environment variables if specified
    if args.qa_mode:
        os.environ["QA_MODE"] = "true"
    if args.qa_breakpoints:
        os.environ["QA_BREAKPOINTS"] = "true"

    try:
        # Initialize ETL controller
        etl_controller = OrchestatedETLController(args.config)

        # Show status if requested
        if args.status:
            status = etl_controller.get_etl_status()
            logger.info("📊 ETL Component Status:")
            logger.info(f"   Census ETL: {status['census_etl']}")
            logger.info(f"   Urban ETL: {status['urban_etl']}")
            logger.info(f"   Location ETL: {status['location_etl']}")
            logger.info(f"   Census years: {status['config_years']['census']}")
            logger.info(f"   Urban years: {status['config_years']['urban']}")
            return

        # Run specific ETL processes based on arguments
        if args.census_only:
            etl_controller.run_census_etl(args.census_begin_year, args.census_end_year)
            
        elif args.urban_only:
            await etl_controller.run_urban_etl(
                args.urban_begin_year, args.urban_end_year, args.urban_endpoints
            )
            
        elif args.location_only:
            etl_controller.run_location_etl(
                args.location_batch_size, args.location_max_coords, args.location_table_name
            )
            
        else:
            # Run complete pipeline
            await etl_controller.run_complete_pipeline(
                census_begin_year=args.census_begin_year,
                census_end_year=args.census_end_year,
                urban_begin_year=args.urban_begin_year,
                urban_end_year=args.urban_end_year,
                urban_endpoints=args.urban_endpoints,
                location_batch_size=args.location_batch_size,
                location_max_coords=args.location_max_coords,
                location_table_name=args.location_table_name,
                skip_census=args.skip_census,
                skip_urban=args.skip_urban,
                skip_location=args.skip_location,
            )

        logger.info("✅ Orchestrated ETL process completed successfully!")

    except Exception as e:
        logger.error(f"❌ Orchestrated ETL process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
