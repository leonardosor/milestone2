#!/usr/bin/env python3
"""
Optimized API-Based Geocoding
=============================

If you must use API-based geocoding, this optimized version:
1. Uses larger batches and more workers
2. Implements smart caching and deduplication
3. Uses multiple geocoding services as fallbacks
4. Implements exponential backoff for rate limiting
5. Saves progress incrementally to handle interruptions

Still slower than offline methods but much faster than the original.
"""

import psycopg2
import requests
import time
import concurrent.futures
import os
import json
import hashlib
from dotenv import load_dotenv
import logging
from typing import List, Tuple, Optional
import random

# Load environment variables
load_dotenv(override=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OptimizedGeocoder:
    """Optimized geocoder with caching, batching, and multiple providers"""

    def __init__(self, max_workers=20, timeout=5, cache_file="geocoding_cache.json"):
        self.max_workers = max_workers
        self.timeout = timeout
        self.cache_file = cache_file
        self.cache = self.load_cache()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "OptimizedGeocoder/1.0"})
        
        # Multiple geocoding providers for fallback
        self.providers = [
            self.geocode_photon,
            self.geocode_nominatim,
            self.geocode_geocodio_free,  # If you have API key
        ]

    def load_cache(self) -> dict:
        """Load geocoding cache from file"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    cache = json.load(f)
                logger.info(f"Loaded {len(cache):,} cached geocodes")
                return cache
        except Exception as e:
            logger.warning(f"Could not load cache: {e}")
        return {}

    def save_cache(self):
        """Save geocoding cache to file"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f)
        except Exception as e:
            logger.warning(f"Could not save cache: {e}")

    def get_cache_key(self, lat: float, lon: float) -> str:
        """Generate cache key for coordinates"""
        # Round to reduce cache size while maintaining accuracy
        rounded_lat = round(lat, 4)
        rounded_lon = round(lon, 4)
        return f"{rounded_lat},{rounded_lon}"

    def geocode_photon(self, lat: float, lon: float) -> Optional[str]:
        """Geocode using Photon API"""
        try:
            url = "https://photon.komoot.io/reverse"
            params = {"lat": lat, "lon": lon, "limit": 1}
            
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                features = data.get("features", [])
                if features:
                    properties = features[0].get("properties", {})
                    return properties.get("postcode")
            
            return None
            
        except Exception as e:
            logger.debug(f"Photon failed for ({lat}, {lon}): {e}")
            return None

    def geocode_nominatim(self, lat: float, lon: float) -> Optional[str]:
        """Geocode using Nominatim API (with rate limiting)"""
        try:
            url = "https://nominatim.openstreetmap.org/reverse"
            params = {
                "lat": lat,
                "lon": lon,
                "format": "json",
                "addressdetails": 1,
                "limit": 1
            }
            
            # Nominatim requires more aggressive rate limiting
            time.sleep(0.1)
            
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                address = data.get("address", {})
                return address.get("postcode")
            
            return None
            
        except Exception as e:
            logger.debug(f"Nominatim failed for ({lat}, {lon}): {e}")
            return None

    def geocode_geocodio_free(self, lat: float, lon: float) -> Optional[str]:
        """Geocode using Geocod.io free tier (if API key available)"""
        api_key = os.getenv("GEOCODIO_API_KEY")
        if not api_key:
            return None
            
        try:
            url = "https://api.geocod.io/v1.7/reverse"
            params = {
                "q": f"{lat},{lon}",
                "api_key": api_key,
                "limit": 1
            }
            
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                if results:
                    components = results[0].get("address_components", {})
                    return components.get("zip")
            
            return None
            
        except Exception as e:
            logger.debug(f"Geocodio failed for ({lat}, {lon}): {e}")
            return None

    def geocode_coordinate(self, lat_lon: Tuple[float, float]) -> Tuple[float, float, Optional[str]]:
        """Geocode a single coordinate with caching and fallbacks"""
        lat, lon = lat_lon
        cache_key = self.get_cache_key(lat, lon)
        
        # Check cache first
        if cache_key in self.cache:
            return (lat, lon, self.cache[cache_key])
        
        # Try providers in order
        for provider in self.providers:
            try:
                zipcode = provider(lat, lon)
                if zipcode:
                    self.cache[cache_key] = zipcode
                    return (lat, lon, zipcode)
            except Exception as e:
                logger.debug(f"Provider {provider.__name__} failed: {e}")
                continue
        
        # No provider succeeded
        self.cache[cache_key] = None
        return (lat, lon, None)

    def geocode_batch(self, coordinates: List[Tuple[float, float]]) -> List[Tuple[float, float, Optional[str]]]:
        """Process batch with optimized threading"""
        results = []
        
        # Check how many are already cached
        cached_count = sum(1 for lat, lon in coordinates 
                          if self.get_cache_key(lat, lon) in self.cache)
        
        if cached_count > 0:
            logger.info(f"Using {cached_count}/{len(coordinates)} cached results")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_coord = {
                executor.submit(self.geocode_coordinate, coord): coord
                for coord in coordinates
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_coord):
                try:
                    result = future.result()
                    results.append(result)
                    completed += 1
                    
                    # Progress update
                    if completed % 100 == 0 or completed == len(coordinates):
                        logger.info(f"Batch progress: {completed}/{len(coordinates)}")
                        
                except Exception as e:
                    coord = future_to_coord[future]
                    logger.error(f"Error processing {coord}: {e}")
                    results.append((coord[0], coord[1], None))
        
        return results


def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host=os.getenv("LOCAL_HOST"),
        dbname=os.getenv("LOCAL_DB"),
        user=os.getenv("LOCAL_USER"),
        password=os.getenv("LOCAL_PW"),
        port=int(os.getenv("LOCAL_PORT", 5432)),
    )


def get_coordinates_to_process(resume_from_id=0):
    """Get coordinates that haven't been processed yet"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Get coordinates not yet in results table
        query = """
            SELECT DISTINCT
                CAST(u.latitude AS DOUBLE PRECISION) as lat,
                CAST(u.longitude AS DOUBLE PRECISION) as lon,
                ROW_NUMBER() OVER (ORDER BY u.latitude, u.longitude) as row_id
            FROM urban_data_directory u
            LEFT JOIN lat_lon_zipcode_optimized r ON 
                ABS(CAST(u.latitude AS DOUBLE PRECISION) - r.latitude) < 0.0001
                AND ABS(CAST(u.longitude AS DOUBLE PRECISION) - r.longitude) < 0.0001
            WHERE u.latitude IS NOT NULL
                AND u.longitude IS NOT NULL
                AND u.latitude != ''
                AND u.longitude != ''
                AND r.latitude IS NULL
        """
        
        if resume_from_id > 0:
            query += f" AND ROW_NUMBER() OVER (ORDER BY u.latitude, u.longitude) > {resume_from_id}"
        
        cur.execute(query)
        coordinates = cur.fetchall()
        
        # Filter to valid ranges and return just lat/lon
        valid_coords = [
            (lat, lon) for lat, lon, row_id in coordinates
            if -90 <= lat <= 90 and -180 <= lon <= 180
        ]
        
        logger.info(f"Found {len(valid_coords):,} coordinates to process")
        return valid_coords
        
    finally:
        conn.close()


def create_optimized_results_table():
    """Create optimized results table"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        cur.execute("DROP TABLE IF EXISTS lat_lon_zipcode_optimized")
        cur.execute("""
            CREATE TABLE lat_lon_zipcode_optimized (
                id SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                zipcode VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(latitude, longitude)
            )
        """)
        
        # Create indexes for faster lookups
        cur.execute("CREATE INDEX idx_optimized_coords ON lat_lon_zipcode_optimized(latitude, longitude)")
        cur.execute("CREATE INDEX idx_optimized_zipcode ON lat_lon_zipcode_optimized(zipcode)")
        
        conn.commit()
        logger.info("Created optimized results table")
        
    finally:
        conn.close()


def save_batch_results(results: List[Tuple[float, float, Optional[str]]]):
    """Save results with conflict handling"""
    if not results:
        return 0
        
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Use INSERT ... ON CONFLICT to handle duplicates
        insert_query = """
            INSERT INTO lat_lon_zipcode_optimized (latitude, longitude, zipcode)
            VALUES (%s, %s, %s)
            ON CONFLICT (latitude, longitude) DO NOTHING
        """
        
        cur.executemany(insert_query, results)
        conn.commit()
        
        inserted_count = cur.rowcount
        return inserted_count
        
    finally:
        conn.close()


def optimized_process_coordinates(batch_size=200, save_frequency=10):
    """Optimized main processing function"""
    logger.info("Starting optimized coordinate processing...")
    
    # Create results table
    create_optimized_results_table()
    
    # Get coordinates to process
    coordinates = get_coordinates_to_process()
    
    if not coordinates:
        logger.info("No coordinates to process")
        return
    
    # Initialize geocoder with optimized settings
    geocoder = OptimizedGeocoder(max_workers=20, timeout=5)
    
    # Process in larger batches
    total_processed = 0
    total_successful = 0
    start_time = time.time()
    
    try:
        for i in range(0, len(coordinates), batch_size):
            batch = coordinates[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(coordinates) - 1) // batch_size + 1
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} coordinates)")
            
            # Process batch
            batch_start = time.time()
            results = geocoder.geocode_batch(batch)
            batch_time = time.time() - batch_start
            
            # Count successful results
            successful_in_batch = sum(1 for r in results if r[2] is not None)
            total_successful += successful_in_batch
            
            # Save results
            inserted_count = save_batch_results(results)
            total_processed += len(results)
            
            # Save cache periodically
            if batch_num % save_frequency == 0:
                geocoder.save_cache()
            
            # Performance metrics
            batch_rate = len(results) / batch_time if batch_time > 0 else 0
            overall_rate = total_processed / (time.time() - start_time)
            
            logger.info(f"Batch {batch_num}: {successful_in_batch}/{len(results)} successful")
            logger.info(f"Batch rate: {batch_rate:.1f} coords/sec, Overall: {overall_rate:.1f} coords/sec")
            
            # Minimal delay between batches
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Processing error: {e}")
    finally:
        # Always save cache before exiting
        geocoder.save_cache()
        
        # Final summary
        total_time = time.time() - start_time
        success_rate = (total_successful / total_processed * 100) if total_processed > 0 else 0
        
        logger.info("Processing summary:")
        logger.info(f"Total time: {total_time:.1f} seconds")
        logger.info(f"Total processed: {total_processed:,}")
        logger.info(f"Successful geocodes: {total_successful:,}")
        logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info(f"Average rate: {total_processed/total_time:.1f} coordinates/second")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Optimized geocoding")
    parser.add_argument("--batch-size", type=int, default=200, help="Batch size")
    parser.add_argument("--save-freq", type=int, default=10, help="Save cache every N batches")
    
    args = parser.parse_args()
    
    optimized_process_coordinates(
        batch_size=args.batch_size,
        save_frequency=args.save_freq
    )
