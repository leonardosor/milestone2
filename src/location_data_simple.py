import psycopg2
import requests
import time
import concurrent.futures
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env
load_dotenv(override=True)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleGeocoder:
    """Simple, fast geocoder using Photon API with minimal overhead"""
    
    def __init__(self, max_workers=10, timeout=10):
        self.max_workers = max_workers
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'LocationDataProcessor/1.0'
        })

    def geocode_coordinate(self, lat_lon):
        """Geocode a single coordinate pair to zipcode"""
        lat, lon = lat_lon
        try:
            url = "https://photon.komoot.io/reverse"
            params = {
                'lat': lat,
                'lon': lon,
                'limit': 1
            }
            
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                features = data.get('features', [])
                
                if features:
                    properties = features[0].get('properties', {})
                    zipcode = properties.get('postcode')
                    return (lat, lon, zipcode)
                
            return (lat, lon, None)
            
        except Exception as e:
            logger.warning(f"Geocoding failed for ({lat}, {lon}): {e}")
            return (lat, lon, None)

    def geocode_batch(self, coordinates):
        """Process a batch of coordinates in parallel"""
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_coord = {
                executor.submit(self.geocode_coordinate, coord): coord 
                for coord in coordinates
            }
            
            for future in concurrent.futures.as_completed(future_to_coord):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    coord = future_to_coord[future]
                    logger.error(f"Error processing {coord}: {e}")
                    results.append((coord[0], coord[1], None))
        
        return results

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host=os.getenv('LOCAL_HOST'),
        dbname=os.getenv('LOCAL_DB'),
        user=os.getenv('LOCAL_USER'),
        password=os.getenv('LOCAL_PW'),
        port=int(os.getenv('LOCAL_PORT', 5432))
    )

def get_coordinates_from_db(limit=None):
    """Get valid coordinates from the database"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Query to get valid coordinates
        query = """
            SELECT DISTINCT 
                CAST(latitude AS DOUBLE PRECISION) as lat,
                CAST(longitude AS DOUBLE PRECISION) as lon
            FROM urban_data_directory 
            WHERE latitude IS NOT NULL 
            AND longitude IS NOT NULL
            AND latitude != ''
            AND longitude != ''
            AND latitude ~ '^-?[0-9]+\.?[0-9]*$'
            AND longitude ~ '^-?[0-9]+\.?[0-9]*$'
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cur.execute(query)
        coordinates = cur.fetchall()
        
        # Filter coordinates within valid ranges
        valid_coords = [
            (lat, lon) for lat, lon in coordinates
            if -90 <= lat <= 90 and -180 <= lon <= 180
        ]
        
        logger.info(f"Retrieved {len(valid_coords)} valid coordinates from database")
        return valid_coords
        
    finally:
        conn.close()

def create_results_table(table_name="lat_lon_zipcode"):
    """Create table to store results"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Drop existing table if it exists
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create new table
        cur.execute(f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                zipcode VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logger.info(f"Created table '{table_name}'")
        
    finally:
        conn.close()

def save_results_to_db(results, table_name="lat_lon_zipcode"):
    """Save geocoding results to database"""
    if not results:
        return 0
    
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Insert results
        insert_query = f"""
            INSERT INTO {table_name} (latitude, longitude, zipcode) 
            VALUES (%s, %s, %s)
        """
        
        cur.executemany(insert_query, results)
        conn.commit()
        
        inserted_count = cur.rowcount
        logger.info(f"Inserted {inserted_count} records into {table_name}")
        return inserted_count
        
    finally:
        conn.close()

def process_coordinates(batch_size=50, max_coordinates=None, table_name="lat_lon_zipcode"):
    """Main processing function"""
    logger.info("Starting coordinate processing...")
    
    # Get coordinates from database
    coordinates = get_coordinates_from_db(limit=max_coordinates)
    
    if not coordinates:
        logger.error("No coordinates found in database")
        return
    
    # Create results table
    create_results_table(table_name)
    
    # Initialize geocoder
    geocoder = SimpleGeocoder(max_workers=5, timeout=15)
    
    # Process coordinates in batches
    total_processed = 0
    total_successful = 0
    start_time = time.time()
    
    for i in range(0, len(coordinates), batch_size):
        batch = coordinates[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(coordinates) - 1) // batch_size + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} coordinates)")
        
        # Geocode batch
        results = geocoder.geocode_batch(batch)
        
        # Count successful results
        successful_in_batch = sum(1 for r in results if r[2] is not None)
        total_successful += successful_in_batch
        
        # Save to database
        inserted_count = save_results_to_db(results, table_name)
        total_processed += inserted_count
        
        logger.info(f"Batch {batch_num}: {successful_in_batch}/{len(results)} successful, {inserted_count} saved")
        
        # Small delay between batches to be respectful to the API
        if i + batch_size < len(coordinates):
            time.sleep(1)
    
    # Final summary
    total_time = time.time() - start_time
    success_rate = (total_successful / total_processed * 100) if total_processed > 0 else 0
    
    logger.info(f"Processing complete!")
    logger.info(f"Total time: {total_time:.1f} seconds")
    logger.info(f"Total processed: {total_processed}")
    logger.info(f"Successful geocodes: {total_successful}")
    logger.info(f"Success rate: {success_rate:.1f}%")
    logger.info(f"Results saved in table: {table_name}")

def test_connection():
    """Test database connection"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        logger.info(f"Database connected: {version[:50]}...")
        
        # Check if source table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'urban_data_directory'
            );
        """)
        
        table_exists = cur.fetchone()[0]
        if table_exists:
            cur.execute("SELECT COUNT(*) FROM urban_data_directory;")
            count = cur.fetchone()[0]
            logger.info(f"Source table 'urban_data_directory' found with {count} records")
        else:
            logger.error("Source table 'urban_data_directory' not found!")
            return False
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Simple lat/lon to zipcode converter')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='Batch size for processing (default: 50)')
    parser.add_argument('--max-coords', type=int, 
                       help='Maximum coordinates to process (default: all)')
    parser.add_argument('--table-name', type=str, default='lat_lon_zipcode',
                       help='Name of results table (default: lat_lon_zipcode)')
    parser.add_argument('--test-only', action='store_true',
                       help='Only test database connection')
    
    args = parser.parse_args()
    
    if args.test_only:
        test_connection()
    else:
        if test_connection():
            process_coordinates(
                batch_size=args.batch_size,
                max_coordinates=args.max_coords,
                table_name=args.table_name
            )
        else:
            logger.error("Cannot proceed without database connection")
