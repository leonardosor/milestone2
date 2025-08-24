#!/usr/bin/env python3
"""
Alternative Fast Geocoding Using Python Libraries
=================================================

Uses offline reverse geocoding libraries that don't require external APIs:
1. offline-geocoder: Uses local data files
2. reverse-geocoder: Offline reverse geocoding
3. geopy with offline providers

Much faster than API calls for bulk processing.
"""

import logging
import os
import time

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host=os.getenv("LOCAL_HOST"),
        dbname=os.getenv("LOCAL_DB"),
        user=os.getenv("LOCAL_USER"),
        password=os.getenv("LOCAL_PW"),
        port=int(os.getenv("LOCAL_PORT", 5432)),
    )


def install_dependencies():
    """Install required packages for offline geocoding"""
    try:
        import subprocess
        import sys

        packages = ["reverse-geocoder", "uszipcode", "geopandas", "shapely"]

        for package in packages:
            logger.info(f"Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

        logger.info("All dependencies installed")
        return True

    except Exception as e:
        logger.error(f"Failed to install dependencies: {e}")
        return False


def geocode_with_uszipcode():
    """Use uszipcode library for fast local geocoding"""
    try:
        # Try alternative import for uszipcode
        try:
            from uszipcode import SearchEngine
        except ImportError as e:
            logger.warning(f"USZipcode not available: {e}")
            return 0

        # Initialize search engine (downloads data first time)
        logger.info("Initializing uszipcode search engine...")
        try:
            search = SearchEngine(simple_or_comprehensive="simple")
        except Exception as e:
            logger.warning(f"Could not initialize USZipcode SearchEngine: {e}")
            return 0

        # Get coordinates from database
        conn = get_db_connection()
        cur = conn.cursor()

        logger.info("Fetching coordinates from database...")
        cur.execute(
            """
            SELECT DISTINCT
                CAST(latitude AS DOUBLE PRECISION) as lat,
                CAST(longitude AS DOUBLE PRECISION) as lon
            FROM urban_data_directory
            WHERE latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND latitude != ''
                AND longitude != ''
        """
        )

        coordinates = cur.fetchall()
        valid_coords = [
            (lat, lon)
            for lat, lon in coordinates
            if -90 <= lat <= 90 and -180 <= lon <= 180
        ]

        logger.info(f"Processing {len(valid_coords):,} coordinates...")

        # Process in batches for better performance
        batch_size = 1000
        results = []

        start_time = time.time()

        for i in range(0, len(valid_coords), batch_size):
            batch = valid_coords[i : i + batch_size]
            batch_results = []

            for lat, lon in batch:
                try:
                    # Find nearest ZIP code
                    result = search.by_coordinates(lat, lon, radius=50, returns=1)

                    if result:
                        zipcode = result[0].zipcode
                        batch_results.append((lat, lon, zipcode))
                    else:
                        batch_results.append((lat, lon, None))

                except Exception as e:
                    logger.debug(f"Error geocoding {lat}, {lon}: {e}")
                    batch_results.append((lat, lon, None))

            results.extend(batch_results)

            # Progress update
            processed = min(i + batch_size, len(valid_coords))
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0

            logger.info(
                f"Processed {processed:,}/{len(valid_coords):,} coordinates ({rate:.0f}/sec)"
            )

        # Save results to database
        logger.info("Saving results to database...")

        # Create results table
        cur.execute("DROP TABLE IF EXISTS lat_lon_zipcode_uszipcode")
        cur.execute(
            """
            CREATE TABLE lat_lon_zipcode_uszipcode (
                id SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                zipcode VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert results
        cur.executemany(
            "INSERT INTO lat_lon_zipcode_uszipcode (latitude, longitude, zipcode) VALUES (%s, %s, %s)",
            results,
        )

        conn.commit()

        # Summary
        total_time = time.time() - start_time
        successful = sum(1 for r in results if r[2] is not None)

        logger.info(f"USZipcode geocoding complete!")
        logger.info(f"Total time: {total_time:.1f} seconds")
        logger.info(f"Total processed: {len(results):,}")
        logger.info(f"Successful: {successful:,}")
        logger.info(f"Success rate: {(successful/len(results)*100):.1f}%")
        logger.info(
            f"Processing rate: {len(results)/total_time:.0f} coordinates/second"
        )

        conn.close()
        return len(results)

    except Exception as e:
        logger.error(f"USZipcode geocoding failed: {e}")
        return 0


def geocode_with_reverse_geocoder():
    """Use reverse-geocoder library for offline geocoding"""
    try:
        import reverse_geocoder as rg

        # Get coordinates from database
        conn = get_db_connection()
        cur = conn.cursor()

        logger.info("Fetching coordinates for reverse geocoding...")
        cur.execute(
            """
            SELECT DISTINCT
                CAST(latitude AS DOUBLE PRECISION) as lat,
                CAST(longitude AS DOUBLE PRECISION) as lon
            FROM urban_data_directory
            WHERE latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND latitude != ''
                AND longitude != ''
        """
        )

        coordinates = cur.fetchall()
        valid_coords = [
            (lat, lon)
            for lat, lon in coordinates
            if -90 <= lat <= 90 and -180 <= lon <= 180
        ]

        logger.info(
            f"Processing {len(valid_coords):,} coordinates with reverse-geocoder..."
        )

        start_time = time.time()

        # Convert to format expected by reverse_geocoder
        coord_list = [(lat, lon) for lat, lon in valid_coords]

        # Batch process all coordinates at once (very fast)
        logger.info("Running bulk reverse geocoding...")
        results = rg.search(coord_list)

        # Extract ZIP codes (note: reverse-geocoder provides admin codes, not ZIP)
        # This will give us administrative regions, which we'll use as a proxy
        processed_results = []
        for i, result in enumerate(results):
            lat, lon = valid_coords[i]
            # Use admin2 (county) or admin1 (state) as available
            admin_code = result.get("admin2", result.get("admin1", None))
            processed_results.append((lat, lon, admin_code))

        # Save results
        logger.info("Saving reverse geocoder results...")

        cur.execute("DROP TABLE IF EXISTS lat_lon_zipcode_reverse_geocoder")
        cur.execute(
            """
            CREATE TABLE lat_lon_zipcode_reverse_geocoder (
                id SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                admin_code VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        cur.executemany(
            "INSERT INTO lat_lon_zipcode_reverse_geocoder (latitude, longitude, admin_code) VALUES (%s, %s, %s)",
            processed_results,
        )

        conn.commit()

        total_time = time.time() - start_time
        successful = sum(1 for r in processed_results if r[2] is not None)

        logger.info(f"Reverse geocoding complete!")
        logger.info(f"Total time: {total_time:.1f} seconds")
        logger.info(f"Total processed: {len(processed_results):,}")
        logger.info(f"Successful: {successful:,}")
        logger.info(
            f"Processing rate: {len(processed_results)/total_time:.0f} coordinates/second"
        )

        conn.close()
        return len(processed_results)

    except Exception as e:
        logger.error(f"Reverse geocoding failed: {e}")
        return 0


def create_zipcode_lookup_table():
    """Create a simple ZIP code lookup based on lat/lon ranges"""
    try:
        # Try alternative import for uszipcode
        try:
            from uszipcode import SearchEngine
        except ImportError as e:
            logger.warning(f"USZipcode not available for lookup table: {e}")
            return 0

        logger.info("Creating ZIP code lookup table...")

        try:
            search = SearchEngine(simple_or_comprehensive="simple")
        except Exception as e:
            logger.warning(
                f"Could not initialize USZipcode SearchEngine for lookup: {e}"
            )
            return 0

        # Get sample ZIP codes instead of all (for performance)
        logger.info("Fetching sample US ZIP codes...")

        conn = get_db_connection()
        cur = conn.cursor()

        # Create lookup table
        cur.execute("DROP TABLE IF EXISTS zipcode_lookup")
        cur.execute(
            """
            CREATE TABLE zipcode_lookup (
                zipcode VARCHAR(10) PRIMARY KEY,
                lat_min DOUBLE PRECISION,
                lat_max DOUBLE PRECISION,
                lon_min DOUBLE PRECISION,
                lon_max DOUBLE PRECISION,
                center_lat DOUBLE PRECISION,
                center_lon DOUBLE PRECISION
            )
        """
        )

        # Get sample ZIP codes by state to ensure coverage
        states = [
            "AL",
            "AK",
            "AZ",
            "AR",
            "CA",
            "CO",
            "CT",
            "DE",
            "FL",
            "GA",
            "HI",
            "ID",
            "IL",
            "IN",
            "IA",
            "KS",
            "KY",
            "LA",
            "ME",
            "MD",
            "MA",
            "MI",
            "MN",
            "MS",
            "MO",
            "MT",
            "NE",
            "NV",
            "NH",
            "NJ",
            "NM",
            "NY",
            "NC",
            "ND",
            "OH",
            "OK",
            "OR",
            "PA",
            "RI",
            "SC",
            "SD",
            "TN",
            "TX",
            "UT",
            "VT",
            "VA",
            "WA",
            "WV",
            "WI",
            "WY",
        ]

        zip_data = []
        for state in states:
            try:
                state_zips = search.by_state(state, returns=50)  # Get 50 per state
                for zip_obj in state_zips:
                    if zip_obj.lat and zip_obj.lng:
                        # Use a small radius around the center point
                        lat_delta = 0.05  # approximately 5.5 km
                        lon_delta = 0.05

                        zip_data.append(
                            (
                                zip_obj.zipcode,
                                zip_obj.lat - lat_delta,  # lat_min
                                zip_obj.lat + lat_delta,  # lat_max
                                zip_obj.lng - lon_delta,  # lon_min
                                zip_obj.lng + lon_delta,  # lon_max
                                zip_obj.lat,  # center_lat
                                zip_obj.lng,  # center_lon
                            )
                        )
            except Exception as e:
                logger.debug(f"Error getting ZIP codes for state {state}: {e}")
                continue

        if not zip_data:
            logger.warning("No ZIP code data retrieved")
            return 0

        cur.executemany(
            """
            INSERT INTO zipcode_lookup
            (zipcode, lat_min, lat_max, lon_min, lon_max, center_lat, center_lon)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (zipcode) DO NOTHING
        """,
            zip_data,
        )

        conn.commit()
        logger.info(f"Created lookup table with {len(zip_data):,} ZIP codes")

        # Now use this for fast lookup
        logger.info("Performing fast lookup geocoding...")

        # Fast geocoding using range queries
        cur.execute("DROP TABLE IF EXISTS lat_lon_zipcode_lookup")
        cur.execute(
            """
            CREATE TABLE lat_lon_zipcode_lookup AS
            SELECT DISTINCT
                CAST(u.latitude AS DOUBLE PRECISION) as latitude,
                CAST(u.longitude AS DOUBLE PRECISION) as longitude,
                z.zipcode,
                CURRENT_TIMESTAMP as created_at
            FROM urban_data_directory u
            JOIN zipcode_lookup z ON
                CAST(u.latitude AS DOUBLE PRECISION) BETWEEN z.lat_min AND z.lat_max
                AND CAST(u.longitude AS DOUBLE PRECISION) BETWEEN z.lon_min AND z.lon_max
            WHERE u.latitude IS NOT NULL
                AND u.longitude IS NOT NULL
                AND u.latitude != ''
                AND u.longitude != ''
        """
        )

        # Add primary key
        cur.execute(
            "ALTER TABLE lat_lon_zipcode_lookup ADD COLUMN id SERIAL PRIMARY KEY"
        )

        # Get results count
        cur.execute("SELECT COUNT(*) FROM lat_lon_zipcode_lookup")
        count = cur.fetchone()[0]

        conn.commit()
        conn.close()

        logger.info(f"Lookup table geocoding complete: {count:,} results")
        return count

    except Exception as e:
        logger.error(f"Lookup table creation failed: {e}")
        return 0


def fast_geocode_coordinates(table_name="location_data"):
    """
    Main function for fast geocoding - uses reverse-geocoder library
    Returns True if successful, False otherwise
    """
    try:
        import reverse_geocoder as rg

        logger.info("Starting fast coordinate geocoding...")

        # Get coordinates from database
        conn = get_db_connection()
        cur = conn.cursor()

        logger.info("Fetching coordinates for reverse geocoding...")
        cur.execute(
            """
            SELECT DISTINCT
                CAST(latitude AS DOUBLE PRECISION) as lat,
                CAST(longitude AS DOUBLE PRECISION) as lon
            FROM urban_data_directory
            WHERE latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND latitude != ''
                AND longitude != ''
        """
        )

        coordinates = cur.fetchall()
        valid_coords = [
            (lat, lon)
            for lat, lon in coordinates
            if -90 <= lat <= 90 and -180 <= lon <= 180
        ]

        if not valid_coords:
            logger.warning("No valid coordinates found to process")
            return False

        logger.info(
            f"Processing {len(valid_coords):,} coordinates with reverse-geocoder..."
        )

        start_time = time.time()

        # Convert to format expected by reverse_geocoder
        coord_list = [(lat, lon) for lat, lon in valid_coords]

        # Batch process all coordinates at once (very fast)
        logger.info("Running bulk reverse geocoding...")
        results = rg.search(coord_list)

        # Process results - extract county/admin info
        processed_results = []
        for i, result in enumerate(results):
            lat, lon = valid_coords[i]
            # Get the best available administrative division
            county = result.get("admin2", "")
            state = result.get("admin1", "")
            country = result.get("cc", "")

            # Store county and state separately
            # Use empty string instead of None for consistency
            county = county if county else ""
            state = state if state else ""

            processed_results.append((lat, lon, county, state))

        # Save results
        logger.info("Saving geocoding results to database...")

        # Drop existing table
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create results table
        cur.execute(
            f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                county VARCHAR(100),
                state VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert results
        cur.executemany(
            f"INSERT INTO {table_name} (latitude, longitude, county, state) VALUES (%s, %s, %s, %s)",
            processed_results,
        )

        conn.commit()

        # Summary
        total_time = time.time() - start_time
        successful = sum(
            1 for r in processed_results if r[2] or r[3]
        )  # County or state available

        logger.info(f"Fast geocoding complete!")
        logger.info(f"Total time: {total_time:.1f} seconds")
        logger.info(f"Total processed: {len(processed_results):,}")
        logger.info(f"Successful: {successful:,}")
        logger.info(f"Success rate: {(successful/len(processed_results)*100):.1f}%")
        logger.info(
            f"Processing rate: {len(processed_results)/total_time:.0f} coordinates/second"
        )
        logger.info(f"Results saved in table: {table_name}")

        conn.close()
        return True

    except Exception as e:
        logger.error(f"Fast geocoding failed: {e}")
        return False


def test_geocoding_connection():
    """Test database connection and check for required data"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if source table exists
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'urban_data_directory'
            );
        """
        )

        table_exists = cur.fetchone()[0]
        if not table_exists:
            logger.error("Source table 'urban_data_directory' not found!")
            return False

        # Check if we have coordinates to process - fixed query
        cur.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT latitude, longitude
                FROM urban_data_directory
                WHERE latitude IS NOT NULL
                    AND longitude IS NOT NULL
                    AND latitude != ''
                    AND longitude != ''
            ) AS distinct_coords
        """
        )

        count = cur.fetchone()[0]
        logger.info(f"Found {count:,} distinct coordinates ready for geocoding")

        conn.close()
        return count > 0

    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


def main():
    """Main execution function - simplified to use only the working approach"""
    logger.info("Starting fast geocoding process...")

    # Test connection first
    if not test_geocoding_connection():
        logger.error("Cannot proceed - database connection or data issues")
        return False

    # Use the reverse geocoder approach (fastest and most reliable)
    logger.info("Using reverse-geocoder for fast coordinate processing...")
    start_time = time.time()

    success = fast_geocode_coordinates("location_data")

    if success:
        total_time = time.time() - start_time
        logger.info(
            f"Fast geocoding completed successfully in {total_time:.1f} seconds"
        )
        return True
    else:
        logger.error("Fast geocoding failed")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fast offline geocoding using reverse-geocoder"
    )
    parser.add_argument(
        "--table-name",
        type=str,
        default="location_data",
        help="Name of results table (default: location_data)",
    )
    parser.add_argument(
        "--test-only", action="store_true", help="Only test database connection"
    )

    args = parser.parse_args()

    if args.test_only:
        success = test_geocoding_connection()
        if success:
            logger.info("✓ Database connection and data check passed")
        else:
            logger.error("✗ Database connection or data check failed")
    else:
        # Run the main geocoding process
        success = main()
        if not success:
            exit(1)
