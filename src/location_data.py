#!/usr/bin/env python3
"""
Offline Geocoding using TIGER/Line Shapefiles

Resolves latitude/longitude pairs to ZIP codes, counties, and states using
TIGER/Line shapefiles for ZCTA, counties, and states.

Dependencies: geopandas, shapely, psycopg2, requests
"""

import json
import logging
import os
import time
import zipfile
from pathlib import Path
from typing import Optional, Tuple

import geopandas as gpd
import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from shapely.geometry import Point

# Load environment variables
load_dotenv(override=True)

# Database schema (set from config)
DB_SCHEMA = None

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
SOURCE_TABLE = "test.urban_ccd_directory_exp"
COORD_PREDICATE = (
    "latitude IS NOT NULL AND longitude IS NOT NULL "
    "AND latitude != '' AND longitude != '' "
    "AND CAST(latitude AS DOUBLE PRECISION) BETWEEN -90 AND 90 "
    "AND CAST(longitude AS DOUBLE PRECISION) BETWEEN -180 AND 180"
)

TIGER_BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2023"
TIGER_FILES = {
    "zcta": ("tl_2023_us_zcta520.zip", "ZCTA520"),
    "county": ("tl_2023_us_county.zip", "COUNTY"),
    "state": ("tl_2023_us_state.zip", "STATE"),
}


def load_config(config_file="config.json"):
    """Load configuration from JSON file"""
    global DB_SCHEMA

    # Check root directory first, then current directory
    script_dir = Path(__file__).parent
    config_path = script_dir.parent / "config.json"
    if not config_path.exists():
        config_path = Path(config_file)

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        logger.info(f"Configuration loaded from: {config_path}")
    except FileNotFoundError:
        logger.warning("Configuration file not found, using defaults")
        config = {
            "env_database": {
                "host": "localhost",
                "port": 5432,
                "database": "milestone2",
                "username": "postgres",
                "password": "123",
            },
            "schema": "public",
        }
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        raise

    DB_SCHEMA = config.get("schema", "public")
    return config


def get_db_connection(config_file="config.json"):
    """Get database connection using config file"""
    config = load_config(config_file)
    db_config = config.get("env_database", config.get("local_database", {}))

    conn = psycopg2.connect(
        host=db_config.get("host", "localhost"),
        dbname=db_config.get("database", "milestone2"),
        user=db_config.get("username", "postgres"),
        password=db_config.get("password", "123"),
        port=db_config.get("port", 5432),
    )

    # Ensure schema exists
    _ensure_schema_exists(conn)
    return conn


def _ensure_schema_exists(conn):
    """Ensure the database schema exists"""
    if DB_SCHEMA:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA};")
        conn.commit()
        logger.info(f"Schema '{DB_SCHEMA}' is ready")


def test_database_connection():
    """Test database connection and check for required data"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if source table exists
        schema, table = SOURCE_TABLE.split(".")
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """,
            (schema, table),
        )

        if not cur.fetchone()[0]:
            logger.error(f"Source table '{SOURCE_TABLE}' not found!")
            return False

        # Check coordinate count
        cur.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT latitude, longitude
                FROM {SOURCE_TABLE}
                WHERE {COORD_PREDICATE}
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


#############################
# TIGER/Line Data Management #
#############################


def _get_data_dir(cli_dir: Optional[str] = None) -> Path:
    return Path(cli_dir or os.getenv("TIGER_DATA_DIR") or "tiger_data").resolve()


def _download_file(url: str, target_dir: Path) -> Path:
    """Download file if it doesn't exist"""
    target_dir.mkdir(parents=True, exist_ok=True)
    fname = url.split("/")[-1]
    file_path = target_dir / fname

    if file_path.exists():
        logger.info(f"Already downloaded: {fname}")
        return file_path

    logger.info(f"Downloading {fname} ...")
    r = requests.get(url, timeout=300, verify=False)
    r.raise_for_status()

    with open(file_path, "wb") as f:
        f.write(r.content)
    logger.info(f"Saved {fname} ({len(r.content)/1_000_000:.1f} MB)")
    return file_path


def _extract_shapefile(zip_path: Path, extract_dir: Path):
    """Extract shapefile if not already extracted"""
    if any(
        (extract_dir / f).exists()
        for f in ["*.shp"]
        if (extract_dir / f).suffix.lower() == ".shp"
    ):
        return

    logger.info(f"Extracting {zip_path.name} ...")
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)


def prepare_datasets(
    data_dir: Path, force_download: bool = False
) -> Tuple[Path, Path, Path]:
    """Prepare TIGER datasets (download and extract)"""
    data_dir.mkdir(parents=True, exist_ok=True)

    if force_download:
        logger.info("Force downloading TIGER datasets...")

    # Download files
    for layer, (zip_name, folder) in TIGER_FILES.items():
        if force_download or not (data_dir / zip_name).exists():
            url = f"{TIGER_BASE_URL}/{folder}/{zip_name}"
            _download_file(url, data_dir)

    # Extract shapefiles
    zcta_dir = data_dir / "zcta"
    county_dir = data_dir / "county"
    state_dir = data_dir / "state"

    _extract_shapefile(data_dir / TIGER_FILES["zcta"][0], zcta_dir)
    _extract_shapefile(data_dir / TIGER_FILES["county"][0], county_dir)
    _extract_shapefile(data_dir / TIGER_FILES["state"][0], state_dir)

    return zcta_dir, county_dir, state_dir


def load_geodata(zcta_dir: Path, county_dir: Path, state_dir: Path):
    """Load and prepare geospatial data"""

    def find_shp_file(directory: Path) -> Path:
        for p in directory.glob("*.shp"):
            return p
        raise FileNotFoundError(f"No .shp file found in {directory}")

    # Load dataframes
    zcta_gdf = gpd.read_file(find_shp_file(zcta_dir))[["ZCTA5CE20", "geometry"]]
    county_gdf = gpd.read_file(find_shp_file(county_dir))[
        ["NAME", "STATEFP", "GEOID", "COUNTYFP", "geometry"]
    ]
    state_gdf = gpd.read_file(find_shp_file(state_dir))[
        ["NAME", "STUSPS", "STATEFP", "geometry"]
    ]

    # Clean FIPS codes (remove leading zeros)
    for gdf in [state_gdf, county_gdf]:
        gdf["STATEFP"] = gdf["STATEFP"].astype(str).str.lstrip("0").replace({"": "0"})
    county_gdf["COUNTYFP"] = county_gdf["COUNTYFP"].astype(str).str.lstrip("0")

    # Convert to WGS84
    for gdf in [zcta_gdf, county_gdf, state_gdf]:
        if gdf.crs is None:
            logger.warning(
                "A layer has no CRS; assuming EPSG:4269 -> converting to EPSG:4326"
            )
            gdf.set_crs(epsg=4269, inplace=True)
        gdf.to_crs(epsg=4326, inplace=True)

    # Build spatial indices
    for gdf in [zcta_gdf, county_gdf, state_gdf]:
        _ = gdf.sindex
    logger.info("Spatial indices prepared (ZCTA, County, State)")

    return zcta_gdf, county_gdf, state_gdf


def save_tiger_to_db(zcta_gdf, county_gdf, state_gdf, table_name="census_geodata"):
    """Save TIGER shapefile data to database table"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        logger.info(f"Creating table {table_name}...")
        cur.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{table_name} CASCADE")

        # Create table
        cur.execute(
            f"""
            CREATE TABLE {DB_SCHEMA}.{table_name} (
                id SERIAL PRIMARY KEY,
                geoid VARCHAR(20),
                name VARCHAR(255),
                layer_type VARCHAR(20),
                state_fips VARCHAR(3),
                county_fips VARCHAR(4),
                geometry GEOMETRY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Prepare data for bulk insert
        records = []

        # ZCTA data
        for _, row in zcta_gdf.iterrows():
            records.append(
                (
                    row["ZCTA5CE20"],
                    row["ZCTA5CE20"],
                    "zcta",
                    None,
                    None,
                    row["geometry"].wkt,
                )
            )

        # County data
        for _, row in county_gdf.iterrows():
            records.append(
                (
                    row["GEOID"],
                    row["NAME"],
                    "county",
                    row["STATEFP"],
                    row["COUNTYFP"],
                    row["geometry"].wkt,
                )
            )

        # State data
        for _, row in state_gdf.iterrows():
            records.append(
                (
                    row["STATEFP"],
                    row["NAME"],
                    "state",
                    row["STATEFP"],
                    None,
                    row["geometry"].wkt,
                )
            )

        # Bulk insert
        cur.executemany(
            f"""
            INSERT INTO {DB_SCHEMA}.{table_name}
            (geoid, name, layer_type, state_fips, county_fips, geometry)
            VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
        """,
            records,
        )

        conn.commit()
        logger.info(
            f"Successfully saved {len(records):,} TIGER records to {table_name}"
        )
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Failed to save TIGER data to database: {e}")
        return False


def spatial_join_points(points_df, zcta_gdf, county_gdf, state_gdf):
    """Perform spatial joins to enrich points with location data"""
    # Create GeoDataFrame from points
    gdf_pts = gpd.GeoDataFrame(
        points_df,
        geometry=[
            Point(lon, lat) for lat, lon in zip(points_df.latitude, points_df.longitude)
        ],
        crs="EPSG:4326",
    )

    # Sequential spatial joins: state -> county -> zcta
    pts_state = gpd.sjoin(
        gdf_pts,
        state_gdf[["NAME", "STATEFP", "geometry"]],
        predicate="within",
        how="left",
        rsuffix="_state",
    )
    pts_state.rename(columns={"NAME": "state", "STATEFP": "state_fips"}, inplace=True)

    pts_county = gpd.sjoin(
        pts_state,
        county_gdf[["NAME", "STATEFP", "COUNTYFP", "geometry"]],
        predicate="within",
        how="left",
        rsuffix="_county",
    )
    pts_county.rename(
        columns={"NAME": "county", "COUNTYFP": "county_fips"}, inplace=True
    )

    pts_zcta = gpd.sjoin(
        pts_county, zcta_gdf, predicate="within", how="left", rsuffix="_zcta"
    )
    pts_zcta.rename(columns={"ZCTA5CE20": "zip"}, inplace=True)

    # Select and clean result columns
    result = pts_zcta[
        ["latitude", "longitude", "zip", "county", "county_fips", "state", "state_fips"]
    ].copy()

    # Clean FIPS codes and fill missing values
    result["state_fips"] = (
        result["state_fips"].astype(str).str.lstrip("0").replace({"": "0"})
    )
    result["county_fips"] = result["county_fips"].astype(str).str.lstrip("0")
    result.fillna("", inplace=True)

    return result


def test_geocoding_connection():
    """Alias for test_database_connection for backward compatibility"""
    return test_database_connection()

    return result


def geocode_coordinates_to_location_data(
    table_name="location_data",
    data_dir: Optional[str] = None,
    force_download: bool = False,
):
    """Geocode coordinates using TIGER/Line shapefiles"""
    try:
        data_path = _get_data_dir(data_dir)
        zcta_dir, county_dir, state_dir = prepare_datasets(data_path, force_download)
        zcta_gdf, county_gdf, state_gdf = load_geodata(zcta_dir, county_dir, state_dir)

        # Save TIGER data to database
        logger.info("Saving TIGER/Line geodata to census_geodata table...")
        if not save_tiger_to_db(
            zcta_gdf, county_gdf, state_gdf, table_name="census_geodata"
        ):
            logger.warning("Failed to save TIGER data, continuing with geocoding...")

        conn = get_db_connection()
        cur = conn.cursor()

        # Fetch coordinates
        logger.info("Fetching coordinates from source table...")
        cur.execute(
            f"""
            SELECT DISTINCT CAST(latitude AS DOUBLE PRECISION) AS latitude,
                            CAST(longitude AS DOUBLE PRECISION) AS longitude
            FROM {SOURCE_TABLE}
            WHERE {COORD_PREDICATE}
        """
        )

        rows = cur.fetchall()
        if not rows:
            logger.warning("No coordinates found to process")
            return False

        coords_df = pd.DataFrame(rows, columns=["latitude", "longitude"])
        logger.info(f"Loaded {len(coords_df):,} coordinate pairs")

        # Perform spatial joins
        start = time.time()
        enriched = spatial_join_points(coords_df, zcta_gdf, county_gdf, state_gdf)
        elapsed = time.time() - start
        zip_coverage = (enriched["zip"] != "").mean() * 100
        logger.info(
            f"Spatial joins completed in {elapsed:.1f}s. ZIP coverage: {zip_coverage:.1f}%"
        )

        # Create result table
        logger.info("Creating location data table...")
        cur.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{table_name} CASCADE")
        cur.execute(
            f"""
            CREATE TABLE {DB_SCHEMA}.{table_name} (
                id SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                zip VARCHAR(10),
                county VARCHAR(100),
                county_fips VARCHAR(3),
                state VARCHAR(100),
                state_fips VARCHAR(3),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert results
        records = list(enriched.itertuples(index=False, name=None))
        cur.executemany(
            f"""
            INSERT INTO {DB_SCHEMA}.{table_name}
            (latitude, longitude, zip, county, county_fips, state, state_fips)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
            records,
        )
        conn.commit()

        # Add indexes
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_lat_lon ON {DB_SCHEMA}.{table_name}(latitude, longitude)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_zip ON {DB_SCHEMA}.{table_name}(zip)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {DB_SCHEMA}.{table_name}(state)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_county_fips ON {DB_SCHEMA}.{table_name}(county_fips)",
        ]
        for idx_sql in indexes:
            try:
                cur.execute(idx_sql)
            except Exception as ie:
                logger.warning(f"Failed to create index: {ie}")
        conn.commit()

        zip_count = (enriched["zip"] != "").sum()
        logger.info(
            f"Inserted {len(records):,} rows. ZIP codes populated: {zip_count:,} ({zip_count/len(records)*100:.1f}%)"
        )
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Geocoding failed: {e}")
        return False


def main(args):
    """Main entry point"""
    logger.info("Starting TIGER/Line geocoding pipeline")

    if args.test_only:
        return test_database_connection()

    if not test_database_connection():
        logger.error("Database prerequisite check failed")
        return False

    start = time.time()
    success = geocode_coordinates_to_location_data(
        table_name=args.table_name,
        data_dir=args.data_dir,
        force_download=args.download_data,
    )

    if success:
        logger.info(f"Pipeline completed in {time.time()-start:.1f}s")
    return success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Offline TIGER/Line geocoder")
    parser.add_argument(
        "--table-name", default="location_data", help="Destination table name"
    )
    parser.add_argument("--data-dir", help="Directory for TIGER shapefiles")
    parser.add_argument(
        "--download-data", action="store_true", help="Force download TIGER datasets"
    )
    parser.add_argument(
        "--test-only", action="store_true", help="Test DB connectivity only"
    )

    args = parser.parse_args()
    success = main(args)
    exit(0 if success else 1)


def test_geocoding_connection():
    """Alias for test_database_connection for backward compatibility"""
    return test_database_connection()


def fast_geocode_coordinates(table_name="location_data"):
    """Fast geocoding using TIGER shapefiles - alias for backward compatibility"""
    return geocode_coordinates_to_location_data(table_name=table_name)
