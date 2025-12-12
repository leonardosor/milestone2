#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Import ConfigLoader
sys.path.append(str(Path(__file__).parent.parent / "config"))
from config_loader import ConfigLoader

from census_data import SimpleCensusETL
from location_data import (geocode_coordinates_to_location_data,
                           test_database_connection)
from urban_data import EndpointETL
from urban_data import load_config as load_urban_config

# Ensure logs directory exists
os.makedirs("/app/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/main_etl.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)
QA_MODE = os.getenv("QA_MODE", "false").lower() == "true"
QA_BREAKPOINTS = os.getenv("QA_BREAKPOINTS", "false").lower() == "true"


def qa_breakpoint(message: str, data: Any = None):
    if QA_BREAKPOINTS:
        logger.info(f"QA BREAKPOINT: {message}")
        if data is not None:
            logger.info(f"Data shape: {getattr(data, 'shape', 'N/A')}")
            logger.info(f"Data columns: {getattr(data, 'columns', 'N/A')}")
        if QA_MODE:
            import pdb

            pdb.set_trace()
        else:
            input(f"Press Enter to continue... (QA: {message})")


class OrchestatedETLController:
    def __init__(self, config_file="config.json"):
        # Use ConfigLoader
        self.config_loader = ConfigLoader(config_file)
        self.config = self.config_loader.config
        logger.info("Configuration loaded successfully using ConfigLoader")
        self.census_etl = None
        self.urban_etl = None
        self._initialize_etl_components()
        self.census_years = self.config.get("etl", {}).get("census_years", [2015, 2019])
        self.urban_years = self.config.get("etl", {}).get("urban_years", [2020, 2023])

    def _initialize_etl_components(self):
        try:
            self.census_etl = SimpleCensusETL(config_file="config.json")
            logger.info("Census ETL component initialized")
            urban_config = load_urban_config("config.json")
            self.urban_etl = EndpointETL(config=urban_config, drop_existing=True)
            logger.info("Urban Institute ETL component initialized")
        except Exception as e:
            logger.error(f"Failed to initialize ETL components: {e}")
            raise

    def run_census_etl(self, begin_year: int = None, end_year: int = None):
        try:
            if begin_year is None:
                begin_year = self.census_years[0]
            if end_year is None:
                end_year = self.census_years[1]

            logger.info(
                f"Starting Census ETL process for years {begin_year}-{end_year}"
            )
            qa_breakpoint("Starting Census ETL process", None)
            self.census_etl.run_etl(begin_year=begin_year, end_year=end_year)
            logger.info("Census ETL process completed successfully")
            return True
        except Exception as e:
            logger.error(f"Census ETL process failed: {e}")
            raise

    async def run_urban_etl(
        self, begin_year: int = None, end_year: int = None, endpoints: list = None
    ):
        try:
            if begin_year is None:
                begin_year = self.urban_years[0]
            if end_year is None:
                end_year = self.urban_years[1]

            logger.info(
                f"Starting Urban Institute ETL process for years {begin_year}-{end_year}"
            )
            qa_breakpoint("Starting Urban Institute ETL process", None)
            stats = await self.urban_etl.ingest(
                begin_year=begin_year, end_year=end_year, endpoint_subset=endpoints
            )
            logger.info(
                f"Urban Institute ETL completed: {stats['rows_inserted']} rows inserted"
            )
            return True
        except Exception as e:
            logger.error(f"Urban Institute ETL process failed: {e}")
            raise

    def run_location_etl(
        self,
        table_name: str = "location_data",
        data_dir: str = None,
        force_download: bool = False,
    ):
        try:
            logger.info(f"Starting Location Data ETL process")
            qa_breakpoint("Starting Location Data ETL process", None)

            if not test_database_connection():
                raise Exception("Database connection failed for location ETL")

            success = geocode_coordinates_to_location_data(
                table_name=table_name, data_dir=data_dir, force_download=force_download
            )
            if not success:
                raise Exception("Geocoding process failed")

            logger.info("Location Data ETL process completed successfully")
            return True
        except Exception as e:
            logger.error(f"Location Data ETL process failed: {e}")
            raise

    async def run_complete_pipeline(
        self,
        census_begin_year: int = None,
        census_end_year: int = None,
        urban_begin_year: int = None,
        urban_end_year: int = None,
        urban_endpoints: list = None,
        location_table_name: str = "location_data",
        location_data_dir: str = None,
        location_force_download: bool = False,
        skip_census: bool = False,
        skip_urban: bool = False,
        skip_location: bool = False,
    ):
        try:
            pipeline_start = datetime.now()
            logger.info("Starting Complete ETL Pipeline")
            logger.info("=" * 60)

            qa_breakpoint("Starting complete ETL pipeline", None)
            if not skip_census:
                logger.info("STAGE 1: Census Data Collection")
                logger.info("-" * 40)
                self.run_census_etl(census_begin_year, census_end_year)
                logger.info("Stage 1 completed\n")
            else:
                logger.info("Skipping Census ETL")
            if not skip_urban:
                logger.info("STAGE 2: Urban Institute Data Collection")
                logger.info("-" * 40)
                await self.run_urban_etl(
                    urban_begin_year, urban_end_year, urban_endpoints
                )
                logger.info("Stage 2 completed\n")
            else:
                logger.info("Skipping Urban ETL")
            if not skip_location:
                logger.info(
                    "STAGE 3: Location Data Processing (Coordinates → Zipcodes)"
                )
                logger.info("-" * 40)
                self.run_location_etl(
                    location_table_name, location_data_dir, location_force_download
                )
                logger.info("Stage 3 completed\n")
            else:
                logger.info("Skipping Location ETL")
            pipeline_end = datetime.now()
            duration = pipeline_end - pipeline_start

            logger.info("=" * 60)
            logger.info("COMPLETE ETL PIPELINE FINISHED SUCCESSFULLY!")
            logger.info(f"Total pipeline duration: {duration}")
            logger.info(f"Completed at: {pipeline_end.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)
        except Exception as e:
            logger.error(f"Complete ETL pipeline failed: {e}")
            raise

    def get_etl_status(self):
        status = {
            "census_etl": "initialized" if self.census_etl else "not_initialized",
            "urban_etl": "initialized" if self.urban_etl else "not_initialized",
            "location_etl": "available",
            "config_years": {"census": self.census_years, "urban": self.urban_years},
        }
        return status


async def main():
    parser = argparse.ArgumentParser(
        description="Orchestrated ETL Controller for Complete Data Pipeline"
    )
    parser.add_argument(
        "--config", type=str, default="config.json", help="Configuration file path"
    )
    parser.add_argument(
        "--census-begin-year", type=int, help="Start year for Census data"
    )
    parser.add_argument("--census-end-year", type=int, help="End year for Census data")
    parser.add_argument(
        "--urban-begin-year", type=int, help="Start year for Urban Institute data"
    )
    parser.add_argument(
        "--urban-end-year", type=int, help="End year for Urban Institute data"
    )
    parser.add_argument(
        "--urban-endpoints", nargs="+", help="Urban Institute endpoints to fetch"
    )
    parser.add_argument(
        "--location-table-name",
        type=str,
        default="location_data",
        help="Location data table name",
    )
    parser.add_argument(
        "--location-data-dir", type=str, help="Directory for TIGER shapefiles"
    )
    parser.add_argument(
        "--location-force-download",
        action="store_true",
        help="Force download TIGER datasets",
    )
    parser.add_argument(
        "--census-only", action="store_true", help="Run only Census ETL"
    )
    parser.add_argument(
        "--urban-only", action="store_true", help="Run only Urban Institute ETL"
    )
    parser.add_argument(
        "--location-only", action="store_true", help="Run only Location Data ETL"
    )
    parser.add_argument("--skip-census", action="store_true", help="Skip Census ETL")
    parser.add_argument("--skip-urban", action="store_true", help="Skip Urban ETL")
    parser.add_argument(
        "--skip-location", action="store_true", help="Skip Location ETL"
    )
    parser.add_argument(
        "--status", action="store_true", help="Show ETL component status"
    )

    args = parser.parse_args()

    try:
        etl_controller = OrchestatedETLController(args.config)
        if args.status:
            status = etl_controller.get_etl_status()
            logger.info("ETL Component Status:")
            logger.info(f"   Census ETL: {status['census_etl']}")
            logger.info(f"   Urban ETL: {status['urban_etl']}")
            logger.info(f"   Location ETL: {status['location_etl']}")
            logger.info(f"   Census years: {status['config_years']['census']}")
            logger.info(f"   Urban years: {status['config_years']['urban']}")
            return
        if args.census_only:
            etl_controller.run_census_etl(args.census_begin_year, args.census_end_year)
        elif args.urban_only:
            await etl_controller.run_urban_etl(
                args.urban_begin_year, args.urban_end_year, args.urban_endpoints
            )
        elif args.location_only:
            etl_controller.run_location_etl(
                args.location_table_name,
                args.location_data_dir,
                args.location_force_download,
            )
        else:
            await etl_controller.run_complete_pipeline(
                census_begin_year=args.census_begin_year,
                census_end_year=args.census_end_year,
                urban_begin_year=args.urban_begin_year,
                urban_end_year=args.urban_end_year,
                urban_endpoints=args.urban_endpoints,
                location_table_name=args.location_table_name,
                location_data_dir=args.location_data_dir,
                location_force_download=args.location_force_download,
                skip_census=args.skip_census,
                skip_urban=args.skip_urban,
                skip_location=args.skip_location,
            )

        logger.info("Orchestrated ETL process completed successfully!")
    except Exception as e:
        logger.error(f"Orchestrated ETL process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
