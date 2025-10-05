
import psycopg2
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv(override=True)


# Get database connection from environment variables
def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv('LOCAL_HOST'),
        dbname=os.getenv('LOCAL_DB'),
        user=os.getenv('LOCAL_USER'),
        password=os.getenv('LOCAL_PW'),
        port=int(os.getenv('LOCA_PORT', 5432))
    )
    return conn

# Get lat/lon from DB using an open connection
def get_lat_lon_from_db(conn):
    cur = conn.cursor()
    cur.execute("SELECT latitude, longitude FROM urban_data_directory;")
    results = cur.fetchall()
    cur.close()
    return results

# Function to create a new table for lat, lon, and zip code
def create_latlon_zip_table(conn, table_name="latlon_zipcode"):
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            zipcode VARCHAR(20)
        );
    """)
    conn.commit()
    cur.close()

# Function to insert a row into the new table
def insert_latlon_zip(conn, latitude, longitude, zipcode, table_name="latlon_zipcode"):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {table_name} (latitude, longitude, zipcode)
        VALUES (%s, %s, %s);
    """, (latitude, longitude, zipcode))
    conn.commit()
    cur.close()

# Function to get ZIP code from latitude and longitude
def latlon_to_zipcode(latitude, longitude):
    geolocator = Nominatim(user_agent="urban_zip_lookup")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1)
    location = reverse((latitude, longitude), exactly_one=True, addressdetails=True)
    if location and 'postcode' in location.raw['address']:
        return location.raw['address']['postcode']
    return None

# Example usage (fill in your DB credentials)
if __name__ == "__main__":
    table_name = "latlon_zipcode"
    conn = get_db_connection()
    create_latlon_zip_table(conn, table_name=table_name)
    latlons = get_lat_lon_from_db(conn)
    for lat, lon in latlons:
        # Skip rows with missing or invalid lat/lon
        try:
            if lat is None or lon is None:
                continue
            lat_f = float(lat)
            lon_f = float(lon)
        except (ValueError, TypeError):
            continue
        zipcode = latlon_to_zipcode(lat_f, lon_f)
        print(f"Lat: {lat_f}, Lon: {lon_f} => ZIP: {zipcode}")
        insert_latlon_zip(conn, latitude=lat_f, longitude=lon_f, zipcode=zipcode, table_name=table_name)
    conn.close()
