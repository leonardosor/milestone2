#!/usr/bin/env python3
"""
FBI Crime Data ETL to PostgreSQL Database
=========================================

This script provides ETL functionality for FBI Crime Data Exchange (CDE) API data.
Fetches agency information and arrest data from the FBI API endpoints.

Features:
- Fetch agencies list by state from FBI CDE API
- Fetch arrest data by agency and offense type
- Save data to PostgreSQL database
- Comprehensive logging and error handling
- Configuration file support

API Endpoints:
- Base URL: https://api.usa.gov/crime/fbi/cde/LATEST
- Agencies: /agencies/byStateAbbr/{state}
- Arrests: /arrest/agency/{ori}/{offense}

Requirements:
- requests
- pandas
- sqlalchemy
- psycopg2-binary
"""

import argparse
import json
import logging
import sys
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("./logs/fbi_etl.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class FBIDataETL:
    """ETL class for FBI Crime Data Exchange API to PostgreSQL"""

    def __init__(self, config_file="config.json"):
        """Initialize the ETL process with configuration"""
        self.config = self._load_config(config_file)
        self.engine = None
        self.base_url = "https://api.usa.gov/crime/fbi/cde/LATEST"
        self.session = requests.Session()
        
        # Common offense types for arrest data
        self.offense_types = [
            "homicide", "rape", "robbery", "aggravated-assault", 
            "burglary", "larceny", "motor-vehicle-theft", "arson",
            "other-assaults", "forgery", "fraud", "embezzlement",
            "stolen-property", "vandalism", "weapons", "prostitution",
            "sex-offenses", "drug-abuse", "gambling", "offenses-against-family",
            "driving-under-influence", "liquor-laws", "drunkenness",
            "disorderly-conduct", "vagrancy", "all-other-offenses"
        ]
        
        # US state abbreviations
        self.state_abbreviations = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
        ]

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
                    "password": "123",
                }
            }
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise

    def connect_to_database(self):
        """Establish connection to PostgreSQL database"""
        try:
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
        """Create FBI data tables if they don't exist"""
        try:
            with self.engine.connect() as conn:
                # Create agencies table
                agencies_table = """
                CREATE TABLE IF NOT EXISTS fbi_agencies (
                    id SERIAL PRIMARY KEY,
                    ori VARCHAR(20) UNIQUE NOT NULL,
                    agency_name TEXT,
                    agency_type_name TEXT,
                    state_abbr VARCHAR(2),
                    state_name TEXT,
                    division_name TEXT,
                    region_name TEXT,
                    population INTEGER,
                    county_name TEXT,
                    latitude DECIMAL(10, 8),
                    longitude DECIMAL(11, 8),
                    nibrs_start_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                # Create arrests table
                arrests_table = """
                CREATE TABLE IF NOT EXISTS fbi_arrests (
                    id SERIAL PRIMARY KEY,
                    ori VARCHAR(20) NOT NULL,
                    offense_type VARCHAR(50) NOT NULL,
                    data_year INTEGER,
                    arrest_count INTEGER,
                    juvenile_male_count INTEGER DEFAULT 0,
                    juvenile_female_count INTEGER DEFAULT 0,
                    adult_male_count INTEGER DEFAULT 0,
                    adult_female_count INTEGER DEFAULT 0,
                    race_data JSONB,
                    ethnicity_data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ori) REFERENCES fbi_agencies(ori)
                );
                """
                
                conn.execute(text(agencies_table))
                conn.execute(text(arrests_table))
                conn.commit()
                
            logger.info("✅ FBI data tables created successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to create tables: {e}")
            raise

    def fetch_agencies_by_state(self, state_abbr: str) -> Optional[List[Dict]]:
        """Fetch agencies for a specific state"""
        try:
            url = f"{self.base_url}/agencies/byStateAbbr/{state_abbr}"
            logger.info(f"Fetching agencies for state: {state_abbr}")
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            agencies = data.get('results', [])
            
            logger.info(f"Found {len(agencies)} agencies for {state_abbr}")
            return agencies
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to fetch agencies for {state_abbr}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON response for {state_abbr}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error fetching agencies for {state_abbr}: {e}")
            return None

    def fetch_all_agencies(self) -> List[Dict]:
        """Fetch agencies for all states"""
        all_agencies = []
        
        logger.info("Starting to fetch agencies for all states...")
        
        for state in self.state_abbreviations:
            try:
                agencies = self.fetch_agencies_by_state(state)
                if agencies:
                    all_agencies.extend(agencies)
                
                # Rate limiting - be respectful to the API
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Error processing state {state}: {e}")
                continue
        
        logger.info(f"✅ Total agencies fetched: {len(all_agencies)}")
        return all_agencies

    def fetch_arrest_data(self, ori: str, offense: str) -> Optional[Dict]:
        """Fetch arrest data for specific agency and offense"""
        try:
            url = f"{self.base_url}/arrest/agency/{ori}/{offense}"
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data.get('results', [])
            
        except requests.exceptions.RequestException as e:
            logger.debug(f"Failed to fetch arrest data for {ori}/{offense}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.debug(f"Invalid JSON response for {ori}/{offense}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Unexpected error fetching arrest data for {ori}/{offense}: {e}")
            return None

    def save_agencies_to_database(self, agencies: List[Dict]):
        """Save agencies data to database"""
        if not agencies:
            logger.warning("No agencies data to save")
            return
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(agencies)
            
            # Rename columns to match database schema
            column_mapping = {
                'ori': 'ori',
                'agency_name': 'agency_name',
                'agency_type_name': 'agency_type_name',
                'state_abbr': 'state_abbr',
                'state_name': 'state_name',
                'division_name': 'division_name',
                'region_name': 'region_name',
                'population': 'population',
                'county_name': 'county_name',
                'latitude': 'latitude',
                'longitude': 'longitude',
                'nibrs_start_date': 'nibrs_start_date'
            }
            
            # Select and rename columns that exist
            df_clean = pd.DataFrame()
            for api_col, db_col in column_mapping.items():
                if api_col in df.columns:
                    df_clean[db_col] = df[api_col]
            
            # Convert data types
            if 'population' in df_clean.columns:
                df_clean['population'] = pd.to_numeric(df_clean['population'], errors='coerce')
            if 'latitude' in df_clean.columns:
                df_clean['latitude'] = pd.to_numeric(df_clean['latitude'], errors='coerce')
            if 'longitude' in df_clean.columns:
                df_clean['longitude'] = pd.to_numeric(df_clean['longitude'], errors='coerce')
            
            # Save to database using upsert logic
            df_clean.to_sql(
                'fbi_agencies', 
                self.engine, 
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"✅ Saved {len(df_clean)} agencies to database")
            
        except Exception as e:
            logger.error(f"❌ Failed to save agencies to database: {e}")
            raise

    def save_arrests_to_database(self, arrest_data: List[Dict], ori: str, offense: str):
        """Save arrest data to database"""
        if not arrest_data:
            return
        
        try:
            records = []
            for record in arrest_data:
                arrest_record = {
                    'ori': ori,
                    'offense_type': offense,
                    'data_year': record.get('data_year'),
                    'arrest_count': record.get('arrest_count', 0),
                    'juvenile_male_count': record.get('juvenile_male_count', 0),
                    'juvenile_female_count': record.get('juvenile_female_count', 0),
                    'adult_male_count': record.get('adult_male_count', 0),
                    'adult_female_count': record.get('adult_female_count', 0),
                    'race_data': json.dumps(record.get('race_data', {})),
                    'ethnicity_data': json.dumps(record.get('ethnicity_data', {}))
                }
                records.append(arrest_record)
            
            if records:
                df = pd.DataFrame(records)
                df.to_sql(
                    'fbi_arrests',
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                logger.debug(f"Saved {len(records)} arrest records for {ori}/{offense}")
                
        except Exception as e:
            logger.error(f"❌ Failed to save arrest data for {ori}/{offense}: {e}")

    def run_agencies_etl(self):
        """Run the agencies ETL process"""
        try:
            logger.info("🏛️ Starting FBI agencies ETL process")
            
            # Connect to database
            self.connect_to_database()
            
            # Create tables
            self.create_tables()
            
            # Fetch all agencies
            agencies = self.fetch_all_agencies()
            
            # Save to database
            if agencies:
                self.save_agencies_to_database(agencies)
                logger.info("✅ FBI agencies ETL process completed successfully")
                return agencies
            else:
                logger.warning("No agencies data retrieved")
                return []
                
        except Exception as e:
            logger.error(f"❌ FBI agencies ETL process failed: {e}")
            logger.error(traceback.format_exc())
            raise

    def run_arrests_etl(self, limit_agencies: int = None, limit_offenses: List[str] = None):
        """Run the arrests ETL process"""
        try:
            logger.info("🚔 Starting FBI arrests ETL process")
            
            # Connect to database if not already connected
            if not self.engine:
                self.connect_to_database()
            
            # Get agencies from database
            query = "SELECT ori, agency_name FROM fbi_agencies"
            if limit_agencies:
                query += f" LIMIT {limit_agencies}"
                
            agencies_df = pd.read_sql(query, self.engine)
            
            offenses_to_fetch = limit_offenses or self.offense_types
            
            logger.info(f"Fetching arrest data for {len(agencies_df)} agencies and {len(offenses_to_fetch)} offense types")
            
            total_records = 0
            for _, agency in agencies_df.iterrows():
                ori = agency['ori']
                agency_name = agency['agency_name']
                
                logger.info(f"Processing arrests for {agency_name} ({ori})")
                
                for offense in offenses_to_fetch:
                    try:
                        arrest_data = self.fetch_arrest_data(ori, offense)
                        if arrest_data:
                            self.save_arrests_to_database(arrest_data, ori, offense)
                            total_records += len(arrest_data)
                        
                        # Rate limiting
                        time.sleep(0.2)
                        
                    except Exception as e:
                        logger.error(f"❌ Error processing {ori}/{offense}: {e}")
                        continue
            
            logger.info(f"✅ FBI arrests ETL process completed. Total records: {total_records}")
            
        except Exception as e:
            logger.error(f"❌ FBI arrests ETL process failed: {e}")
            logger.error(traceback.format_exc())
            raise

    def run_full_etl(self, limit_agencies: int = None, limit_offenses: List[str] = None):
        """Run complete FBI data ETL process"""
        try:
            logger.info("🚨 Starting complete FBI data ETL process")
            start_time = datetime.now()
            
            # Run agencies ETL first
            agencies = self.run_agencies_etl()
            
            # Then run arrests ETL
            if agencies:
                self.run_arrests_etl(limit_agencies=limit_agencies, limit_offenses=limit_offenses)
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info(f"✅ Complete FBI ETL process finished in {duration}")
            
        except Exception as e:
            logger.error(f"❌ Complete FBI ETL process failed: {e}")
            raise
        finally:
            if self.session:
                self.session.close()


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="FBI Crime Data ETL")
    parser.add_argument(
        "--mode",
        choices=["agencies", "arrests", "full"],
        default="full",
        help="ETL mode: agencies only, arrests only, or full pipeline"
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--limit-agencies",
        type=int,
        help="Limit number of agencies for arrests ETL (for testing)"
    )
    parser.add_argument(
        "--offenses",
        nargs="+",
        help="Specific offense types to fetch (default: all)"
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize ETL
        etl = FBIDataETL(config_file=args.config)
        
        if args.mode == "agencies":
            etl.run_agencies_etl()
        elif args.mode == "arrests":
            etl.run_arrests_etl(
                limit_agencies=args.limit_agencies,
                limit_offenses=args.offenses
            )
        else:  # full
            etl.run_full_etl(
                limit_agencies=args.limit_agencies,
                limit_offenses=args.offenses
            )
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()