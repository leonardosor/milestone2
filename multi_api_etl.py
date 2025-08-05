#!/usr/bin/env python3
"""
Multi-API ETL System for Census and Urban Institute Data
========================================================

This script provides a comprehensive ETL system that can pull data from multiple APIs:
- US Census Bureau API (via censusdata library)
- Urban Institute API (urban.org)

Features:
- Async processing for better performance
- PostgreSQL database integration
- AWS integration
- Comprehensive error handling
- Rate limiting and retry logic
- Configurable data sources

Requirements:
- psycopg2-binary
- boto3
- censusdata
- uszipcode
- pandas
- sqlalchemy
- aiohttp
- asyncio
- requests
- backoff
"""

import os
import sys
import logging
import pandas as pd
import censusdata
import uszipcode
from uszipcode import SearchEngine
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import json
from datetime import datetime
import time
import asyncio
import aiohttp
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Union
import backoff
from dataclasses import dataclass
from collections import defaultdict
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('multi_api_etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class APIRequest:
    """Data class for API requests"""
    source: str  # 'census' or 'urban'
    endpoint: str
    parameters: Dict[str, Any]
    year: Optional[int] = None
    geography: Optional[str] = None

class DataSource(ABC):
    """Abstract base class for data sources"""
    
    @abstractmethod
    async def fetch_data(self, request: APIRequest) -> Optional[pd.DataFrame]:
        """Fetch data from the API"""
        pass
    
    @abstractmethod
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process and transform the data"""
        pass

class CensusDataSource(DataSource):
    """Census API data source implementation"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.census_variables = {
            'B02001_001E': 'Total Pop Estimate',
            'B19001_016E': 'HHI 150K-200K',
            'B19001_017E': 'HHI 220K+',
            'B01001_006E': 'Males 15-17',
            'B01001_030E': 'Females 15-17',
            'B01001A_006E': 'White Males 15-17',
            'B01001B_006E': 'Black Males 15-17',
            'B01001I_006E': 'Hispanic Males 15-17',
            'B01001A_021E': 'White Females 15-17',
            'B01001B_021E': 'Black Females 15-17',
            'B01001I_021E': 'Hispanic Females 15-17'
        }
    
    async def fetch_data(self, request: APIRequest) -> Optional[pd.DataFrame]:
        """Fetch Census data using the censusdata library"""
        try:
            logger.info(f"Fetching Census data for year {request.year}")
            
            # Use thread pool for synchronous censusdata library
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                census_data = await loop.run_in_executor(
                    executor,
                    self._sync_census_request,
                    request.year
                )
            
            if census_data is not None and not census_data.empty:
                logger.info(f"Successfully fetched {len(census_data)} Census records for year {request.year}")
                return census_data
            else:
                logger.warning(f"No Census data returned for year {request.year}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to fetch Census data for year {request.year}: {e}")
            return None
    
    def _sync_census_request(self, year: int) -> Optional[pd.DataFrame]:
        """Synchronous Census API request"""
        try:
            llaves = sorted(list(self.census_variables.keys()), reverse=True)
            
            census_data = censusdata.download(
                'acs5', 
                year, 
                censusdata.censusgeo([('state', '*'), ('zip code tabulation area', '*')]), 
                llaves
            )
            
            census_data.rename(columns=self.census_variables, inplace=True)
            census_data.reset_index(inplace=True)
            census_data['zip code'] = census_data['index'].apply(lambda x: x.params()[1][1])
            census_data['year'] = year
            census_data['data_source'] = 'census'
            census_data.drop(columns='index', inplace=True)
            
            return census_data
            
        except Exception as e:
            logger.error(f"Sync Census request failed for year {year}: {e}")
            return None
    
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process Census data"""
        if data.empty:
            return data
        
        # Add any additional processing specific to Census data
        data['processed_at'] = datetime.now()
        return data

class UrbanInstituteDataSource(DataSource):
    """Urban Institute API data source implementation"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.base_url = "https://api.urban.org"
        self.api_key = config.get('urban', {}).get('api_key', '')
        self.session = None
    
    async def _create_session(self):
        """Create aiohttp session for Urban Institute API"""
        if not self.session:
            connector = aiohttp.TCPConnector(
                limit=50,
                limit_per_host=20,
                ttl_dns_cache=300,
                use_dns_cache=True
            )
            timeout = aiohttp.ClientTimeout(total=60, connect=10)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={
                    'User-Agent': 'Multi-API-ETL/1.0',
                    'Authorization': f'Bearer {self.api_key}' if self.api_key else ''
                }
            )
    
    async def _close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None
    
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=300
    )
    async def fetch_data(self, request: APIRequest) -> Optional[pd.DataFrame]:
        """Fetch data from Urban Institute API"""
        try:
            await self._create_session()
            
            # Build URL with parameters
            url = f"{self.base_url}{request.endpoint}"
            params = request.parameters.copy()
            
            logger.info(f"Fetching Urban Institute data from {url}")
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Convert to DataFrame
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    elif isinstance(data, dict) and 'data' in data:
                        df = pd.DataFrame(data['data'])
                    else:
                        df = pd.DataFrame([data])
                    
                    if not df.empty:
                        df['data_source'] = 'urban_institute'
                        df['year'] = request.year if request.year else datetime.now().year
                        df['fetched_at'] = datetime.now()
                        
                        logger.info(f"Successfully fetched {len(df)} Urban Institute records")
                        return df
                    else:
                        logger.warning("No data returned from Urban Institute API")
                        return None
                else:
                    logger.error(f"Urban Institute API error: {response.status} - {await response.text()}")
                    return None
                    
        except Exception as e:
            logger.error(f"Failed to fetch Urban Institute data: {e}")
            return None
    
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process Urban Institute data"""
        if data.empty:
            return data
        
        # Add any additional processing specific to Urban Institute data
        data['processed_at'] = datetime.now()
        return data

class MultiAPIDataETL:
    """Multi-API ETL class for Census and Urban Institute data"""
    
    def __init__(self, config_file='config.json'):
        """Initialize the multi-API ETL process"""
        self.config = self._load_config(config_file)
        self.engine = None
        self.session = None
        self.semaphore = None
        self.data_sources = {}
        self._initialize_data_sources()
    
    def _load_config(self, config_file):
        """Load configuration from JSON file"""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            logger.info("Configuration loaded successfully")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file {config_file} not found")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise
    
    def _initialize_data_sources(self):
        """Initialize data source handlers"""
        self.data_sources['census'] = CensusDataSource(self.config)
        self.data_sources['urban'] = UrbanInstituteDataSource(self.config)
        logger.info("Data sources initialized")
    
    def _get_aws_secrets(self):
        """Retrieve database credentials from AWS Secrets Manager"""
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=self.config['aws']['region']
            )
            
            secret_name = self.config['aws']['secret_name']
            response = client.get_secret_value(SecretId=secret_name)
            secret = json.loads(response['SecretString'])
            
            logger.info("AWS secrets retrieved successfully")
            return secret
        except Exception as e:
            logger.error(f"Failed to retrieve AWS secrets: {e}")
            raise
    
    def connect_to_database(self):
        """Establish connection to PostgreSQL database (local or AWS RDS)"""
        try:
            database_type = self.config.get('database_type', 'aws')
            logger.info(f"Connecting to {database_type} database")
            
            if database_type == 'aws':
                # Get credentials from AWS Secrets Manager or use AWS config
                if self.config.get('use_aws_secrets', False):
                    db_creds = self._get_aws_secrets()
                else:
                    db_creds = self.config['database']
            elif database_type == 'local':
                # Use local database configuration
                db_creds = self.config['local_database']
            else:
                raise ValueError(f"Invalid database_type: {database_type}. Must be 'local' or 'aws'")
            
            # Create connection string
            connection_string = (
                f"postgresql://{db_creds['username']}:{db_creds['password']}"
                f"@{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
            )
            
            # Get connection pool settings from config
            async_config = self.config.get('async', {})
            pool_size = async_config.get('connection_pool_size', 10)
            max_overflow = async_config.get('max_overflow', 20)
            
            # Create SQLAlchemy engine
            self.engine = create_engine(
                connection_string,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Database connection established successfully to {database_type} database")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    async def _create_session(self):
        """Create aiohttp session with connection pooling"""
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=30,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'Multi-API-ETL/1.0'}
        )
        
        # Create semaphore for rate limiting
        max_concurrent = self.config.get('async', {}).get('max_concurrent_requests', 10)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        logger.info(f"Created async session with {max_concurrent} concurrent requests limit")
    
    async def _close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            logger.info("Async session closed")
    
    async def _fetch_data_async(self, request: APIRequest) -> Optional[pd.DataFrame]:
        """Fetch data asynchronously with rate limiting"""
        async with self.semaphore:
            try:
                data_source = self.data_sources.get(request.source)
                if not data_source:
                    logger.error(f"Unknown data source: {request.source}")
                    return None
                
                # Fetch data
                data = await data_source.fetch_data(request)
                
                if data is not None and not data.empty:
                    # Process data
                    processed_data = data_source.process_data(data)
                    return processed_data
                else:
                    return None
                    
            except Exception as e:
                logger.error(f"Failed to fetch data for {request.source}: {e}")
                return None
    
    async def fetch_census_data_async(self, begin_year: int, end_year: int) -> pd.DataFrame:
        """Fetch Census data for a range of years"""
        try:
            logger.info(f"Fetching Census data for years {begin_year}-{end_year}")
            
            requests = []
            for year in range(begin_year, end_year):
                request = APIRequest(
                    source='census',
                    endpoint='acs5',
                    parameters={},
                    year=year,
                    geography='zip code tabulation area'
                )
                requests.append(request)
            
            # Process requests concurrently
            tasks = [self._fetch_data_async(req) for req in requests]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out None results and exceptions
            valid_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Census request for year {begin_year + i} failed: {result}")
                elif result is not None:
                    valid_results.append(result)
            
            if valid_results:
                consolidated_data = pd.concat(valid_results, ignore_index=True)
                consolidated_data.reset_index(drop=True, inplace=True)
                
                logger.info(f"Total Census records consolidated: {len(consolidated_data)}")
                return consolidated_data
            else:
                logger.warning("No Census data was fetched")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to fetch Census data: {e}")
            raise
    
    async def fetch_urban_data_async(self, endpoints: List[Dict[str, Any]]) -> pd.DataFrame:
        """Fetch Urban Institute data for specified endpoints"""
        try:
            logger.info(f"Fetching Urban Institute data for {len(endpoints)} endpoints")
            
            requests = []
            for endpoint_config in endpoints:
                request = APIRequest(
                    source='urban',
                    endpoint=endpoint_config['endpoint'],
                    parameters=endpoint_config.get('parameters', {}),
                    year=endpoint_config.get('year')
                )
                requests.append(request)
            
            # Process requests concurrently
            tasks = [self._fetch_data_async(req) for req in requests]
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
                
                logger.info(f"Total Urban Institute records consolidated: {len(consolidated_data)}")
                return consolidated_data
            else:
                logger.warning("No Urban Institute data was fetched")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to fetch Urban Institute data: {e}")
            raise
    
    def create_tables(self):
        """Create database tables for multi-API data"""
        try:
            with self.engine.connect() as conn:
                # Create census_data table
                census_table_sql = """
                CREATE TABLE IF NOT EXISTS census_data (
                    id SERIAL PRIMARY KEY,
                    zip_code VARCHAR(10),
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
                    data_source VARCHAR(20) DEFAULT 'census',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                # Create urban_institute_data table
                urban_table_sql = """
                CREATE TABLE IF NOT EXISTS urban_institute_data (
                    id SERIAL PRIMARY KEY,
                    data_source VARCHAR(20) DEFAULT 'urban_institute',
                    year INTEGER,
                    endpoint VARCHAR(255),
                    data_json JSONB,
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
                CREATE INDEX IF NOT EXISTS idx_census_data_source ON census_data(data_source);
                CREATE INDEX IF NOT EXISTS idx_urban_data_source ON urban_institute_data(data_source);
                CREATE INDEX IF NOT EXISTS idx_urban_data_year ON urban_institute_data(year);
                CREATE INDEX IF NOT EXISTS idx_locale_data_zip ON locale_data(zip_code);
                CREATE INDEX IF NOT EXISTS idx_locale_data_state ON locale_data(state);
                """
                
                conn.execute(text(census_table_sql))
                conn.execute(text(urban_table_sql))
                conn.execute(text(locale_table_sql))
                conn.execute(text(indexes_sql))
                conn.commit()
                
            logger.info("Database tables created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    async def insert_census_data_async(self, census_data: pd.DataFrame):
        """Insert census data into database"""
        if census_data.empty:
            logger.warning("No census data to insert")
            return
        
        try:
            logger.info(f"Inserting {len(census_data)} census records into database")
            
            # Prepare data for insertion
            census_data_db = census_data.copy()
            census_data_db.columns = [col.lower().replace(' ', '_').replace('(', '').replace(')', '') 
                                    for col in census_data_db.columns]
            
            # Rename columns to match database schema
            column_mapping = {
                'zip code': 'zip_code',
                'total pop estimate': 'total_pop_estimate',
                'hhi 150k-200k': 'hhi_150k_200k',
                'hhi 220k+': 'hhi_220k_plus',
                'males 15-17': 'males_15_17',
                'females 15-17': 'females_15_17',
                'white males 15-17': 'white_males_15_17',
                'black males 15-17': 'black_males_15_17',
                'hispanic males 15-17': 'hispanic_males_15_17',
                'white females 15-17': 'white_females_15_17',
                'black females 15-17': 'black_females_15_17',
                'hispanic females 15-17': 'hispanic_females_15_17'
            }
            
            census_data_db.rename(columns=column_mapping, inplace=True)
            
            # Insert data in batches
            batch_size = self.config.get('async', {}).get('db_batch_size', 1000)
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                for i in range(0, len(census_data_db), batch_size):
                    batch = census_data_db.iloc[i:i+batch_size]
                    
                    await loop.run_in_executor(
                        executor,
                        self._insert_census_batch_sync,
                        batch
                    )
                    
                    logger.info(f"Inserted census batch {i//batch_size + 1}: {len(batch)} records")
            
            logger.info("Successfully inserted census data")
            
        except Exception as e:
            logger.error(f"Failed to insert census data: {e}")
            raise
    
    def _insert_census_batch_sync(self, batch: pd.DataFrame):
        """Synchronous census batch insertion"""
        with self.engine.connect() as conn:
            batch.to_sql('census_data', conn, if_exists='append', index=False, method='multi')
    
    async def insert_urban_data_async(self, urban_data: pd.DataFrame):
        """Insert Urban Institute data into database"""
        if urban_data.empty:
            logger.warning("No Urban Institute data to insert")
            return
        
        try:
            logger.info(f"Inserting {len(urban_data)} Urban Institute records into database")
            
            # Process data in batches
            batch_size = self.config.get('async', {}).get('db_batch_size', 1000)
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                for i in range(0, len(urban_data), batch_size):
                    batch = urban_data.iloc[i:i+batch_size]
                    
                    await loop.run_in_executor(
                        executor,
                        self._insert_urban_batch_sync,
                        batch
                    )
                    
                    logger.info(f"Inserted urban batch {i//batch_size + 1}: {len(batch)} records")
            
            logger.info("Successfully inserted Urban Institute data")
            
        except Exception as e:
            logger.error(f"Failed to insert Urban Institute data: {e}")
            raise
    
    def _insert_urban_batch_sync(self, batch: pd.DataFrame):
        """Synchronous Urban Institute batch insertion"""
        with self.engine.connect() as conn:
            for _, row in batch.iterrows():
                # Convert row to JSON for storage
                data_json = row.to_dict()
                
                insert_sql = """
                INSERT INTO urban_institute_data (data_source, year, endpoint, data_json)
                VALUES (%s, %s, %s, %s)
                """
                
                conn.execute(text(insert_sql), (
                    row.get('data_source', 'urban_institute'),
                    row.get('year'),
                    row.get('endpoint', ''),
                    json.dumps(data_json)
                ))
            
            conn.commit()
    
    async def create_locale_data_async(self, census_data: pd.DataFrame) -> pd.DataFrame:
        """Create location mapping data"""
        try:
            logger.info("Creating locale data mapping")
            
            unique_zips = pd.DataFrame(census_data['zip code'].drop_duplicates())
            search_engine = SearchEngine()
            
            # Process zip codes in batches
            batch_size = self.config.get('async', {}).get('locale_batch_size', 100)
            zip_batches = [
                unique_zips['zip code'].iloc[i:i+batch_size].tolist()
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
                        executor,
                        self._process_zip_batch,
                        zip_batch,
                        search_engine
                    )
                
                all_states.extend(batch_results['states'])
                all_cities.extend(batch_results['cities'])
                
                # Small delay between batches
                await asyncio.sleep(0.1)
            
            unique_zips['state'] = all_states
            unique_zips['city'] = all_cities
            
            logger.info(f"Created locale mapping for {len(unique_zips)} unique zip codes")
            return unique_zips
            
        except Exception as e:
            logger.error(f"Failed to create locale data: {e}")
            raise
    
    def _process_zip_batch(self, zip_codes: List[str], search_engine: SearchEngine) -> Dict[str, List[str]]:
        """Process a batch of zip codes"""
        states = []
        cities = []
        
        for zip_code in zip_codes:
            try:
                result = search_engine.by_zipcode(int(zip_code))
                states.append(result.state if result else None)
                cities.append(result.major_city if result else None)
            except:
                states.append(None)
                cities.append(None)
        
        return {'states': states, 'cities': cities}
    
    async def run_etl_async(self, 
                           census_years: tuple = (2015, 2019),
                           urban_endpoints: List[Dict[str, Any]] = None):
        """Run the complete multi-API ETL process"""
        try:
            logger.info("Starting multi-API ETL process")
            
            # Step 1: Connect to database
            self.connect_to_database()
            
            # Step 2: Create tables
            self.create_tables()
            
            # Step 3: Create async session
            await self._create_session()
            
            try:
                # Step 4: Fetch data from all sources
                tasks = []
                
                # Census data task
                if census_years:
                    tasks.append(self.fetch_census_data_async(census_years[0], census_years[1]))
                
                # Urban Institute data task
                if urban_endpoints:
                    tasks.append(self.fetch_urban_data_async(urban_endpoints))
                
                # Execute all data fetching tasks concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                census_data = pd.DataFrame()
                urban_data = pd.DataFrame()
                
                # Process results
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Data fetching task {i} failed: {result}")
                    elif isinstance(result, pd.DataFrame) and not result.empty:
                        if i == 0 and census_years:  # First result is census data
                            census_data = result
                        else:  # Second result is urban data
                            urban_data = result
                
                # Step 5: Create locale data if we have census data
                locale_data = pd.DataFrame()
                if not census_data.empty:
                    locale_data = await self.create_locale_data_async(census_data)
                
                # Step 6: Insert data into database
                insert_tasks = []
                
                if not census_data.empty:
                    insert_tasks.append(self.insert_census_data_async(census_data))
                
                if not urban_data.empty:
                    insert_tasks.append(self.insert_urban_data_async(urban_data))
                
                if insert_tasks:
                    await asyncio.gather(*insert_tasks)
                
                # Step 7: Save backup CSV files
                await self._save_backup_files_async(census_data, urban_data, locale_data)
                
                logger.info("Multi-API ETL process completed successfully")
                
            finally:
                # Step 8: Clean up
                await self._close_session()
                
        except Exception as e:
            logger.error(f"Multi-API ETL process failed: {e}")
            raise
        finally:
            if self.engine:
                self.engine.dispose()
    
    async def _save_backup_files_async(self, census_data: pd.DataFrame, 
                                      urban_data: pd.DataFrame, 
                                      locale_data: pd.DataFrame):
        """Save backup files asynchronously"""
        try:
            logger.info("Saving backup files")
            
            # Save files in thread pool
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                tasks = []
                
                if not census_data.empty:
                    tasks.append(loop.run_in_executor(
                        executor,
                        census_data.to_csv,
                        'census_data_multi.csv',
                        index=False,
                        sep='\t'
                    ))
                
                if not urban_data.empty:
                    tasks.append(loop.run_in_executor(
                        executor,
                        urban_data.to_csv,
                        'urban_institute_data.csv',
                        index=False
                    ))
                
                if not locale_data.empty:
                    tasks.append(loop.run_in_executor(
                        executor,
                        locale_data.to_csv,
                        'locale_data_multi.csv',
                        index=False
                    ))
                
                if tasks:
                    await asyncio.gather(*tasks)
            
            logger.info("Backup files saved successfully")
            
        except Exception as e:
            logger.error(f"Failed to save backup files: {e}")
            raise

async def main():
    """Main async function to run the multi-API ETL process"""
    try:
        # Initialize ETL process
        etl = MultiAPIDataETL()
        
        # Define Urban Institute endpoints to fetch
        urban_endpoints = [
            {
                'endpoint': '/v1/education/schools',
                'parameters': {'limit': 1000},
                'year': 2023
            },
            {
                'endpoint': '/v1/education/districts',
                'parameters': {'limit': 1000},
                'year': 2023
            }
            # Add more Urban Institute endpoints as needed
        ]
        
        # Run ETL process
        await etl.run_etl_async(
            census_years=(2015, 2019),
            urban_endpoints=urban_endpoints
        )
        
        print("Multi-API ETL process completed successfully!")
        
    except Exception as e:
        logger.error(f"Multi-API ETL process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 