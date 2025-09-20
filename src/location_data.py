#!/usr/bin/env python3
"""
Offline Geocoding (ZIP / County / State) using TIGER/Line Shapefiles
===================================================================

This module resolves latitude/longitude pairs to:
    - ZIP Code (ZCTA5)
    - County Name
    - State Name & State FIPS

Approach:
    1. Download (or expect) TIGER/Line 2023 shapefiles for ZCTA (ZIP Code Tabulation Areas), Counties, and States.
    2. Load them with GeoPandas (shapely geometries) and build spatial indices.
    3. Batch points (coordinates) into a GeoDataFrame and perform spatial joins (point-in-polygon) for ZCTA, county, and state.

Notes:
    - ZCTA polygons approximate USPS ZIP Code delivery areas; they are not exact USPS ZIP boundaries.
    - For performance (≈28k points), spatial joins with indices are efficient.
    - All processing is offline after initial download.

CLI Flags:
    --download-data  : Force (re)download TIGER shapefiles.
    --data-dir PATH  : Custom directory to store shapefiles (default: ./tiger_data)
    --table-name NAME: Destination table (default: location_data)
    --test-only      : Only test DB connectivity & source coordinate availability

Environment Variables (optional overrides):
    TIGER_DATA_DIR   : Directory for shapefiles (overridden by --data-dir if supplied)

Outputs:
    Creates (or replaces) table with columns:
        id, latitude, longitude, zip, county, state, state_fips, created_at

Dependencies (add to requirements.txt if missing):
    geopandas, shapely, fiona, pyproj, rtree (optional, speeds up), tqdm, requests
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

# Database schema configuration - read from config file
DB_SCHEMA = None  # Will be set from config

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

#########################
# Constants / SQL Fragments
#########################

# Source table & schema (currently hard-coded; could be config-driven later)
SOURCE_TABLE = "test.urban_ccd_directory_exp"

# Shared coordinate validation predicate (string fragment, no leading WHERE)
COORD_PREDICATE = (
    "latitude IS NOT NULL AND longitude IS NOT NULL "
    "AND latitude != '' AND longitude != '' "
    "AND CAST(latitude AS DOUBLE PRECISION) BETWEEN -90 AND 90 "
    "AND CAST(longitude AS DOUBLE PRECISION) BETWEEN -180 AND 180"
)


def load_config(config_file="config.json"):
    """Load configuration from JSON file"""
    global DB_SCHEMA

    # Force look for config.json in the root folder (parent of src directory)
    script_dir = Path(__file__).parent
    root_dir = script_dir.parent
    root_config_path = root_dir / "config.json"

    config_path = None
    if root_config_path.exists():
        config_path = root_config_path
        logger.info(f"Found config.json in root folder: {root_config_path}")
    else:
        # Fallback to original behavior
        config_path = Path(config_file)
        if not config_path.exists():
            logger.warning(
                f"Configuration file {config_file} not found in current directory"
            )

    try:
        if config_path and config_path.exists():
            with open(config_path, "r") as f:
                config = json.load(f)
            logger.info(f"Configuration loaded from: {config_path}")
        else:
            raise FileNotFoundError(f"Configuration file not found")
        # Set global schema from config
        DB_SCHEMA = config.get("schema", "public")
        return config
    except FileNotFoundError:
        logger.warning(f"Configuration file not found, using defaults")
        default_config = {
            "env_database": {
                "host": "localhost",
                "port": 5432,
                "database": "milestone2",
                "username": "postgres",
                "password": "123",
            },
            "schema": "public",
        }
        DB_SCHEMA = default_config.get("schema", "public")
        return default_config
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        raise


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
    if DB_SCHEMA:
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA};")
            conn.commit()
            logger.info(f"Schema '{DB_SCHEMA}' is ready")
        except Exception as e:
            logger.error(f"Failed ensuring schema '{DB_SCHEMA}': {e}")
            raise
    return conn


def test_database_connection():
    """Test database connection and check for required data"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if source table exists
        cur.execute(
            f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{{schema}}' AND table_name = '{{table}}'
            );
        """.replace(
                "{schema}", SOURCE_TABLE.split(".")[0]
            ).replace(
                "{table}", SOURCE_TABLE.split(".")[1]
            )
        )

        table_exists = cur.fetchone()[0]
        if not table_exists:
            logger.error("Source table 'test.urban_ccd_directory_exp' not found!")
            return False

        # Check if we have coordinates to process
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

TIGER_BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2023"
ZCTA_ZIP = "tl_2023_us_zcta520.zip"
COUNTY_ZIP = "tl_2023_us_county.zip"
STATE_ZIP = "tl_2023_us_state.zip"


def _get_data_dir(cli_dir: Optional[str] = None) -> Path:
    return Path(cli_dir or os.getenv("TIGER_DATA_DIR") or "tiger_data").resolve()


def _download_zip(url: str, target_dir: Path):
    target_dir.mkdir(parents=True, exist_ok=True)
    fname = url.split("/")[-1]
    zip_path = target_dir / fname
    if zip_path.exists():
        logger.info(f"Already downloaded: {fname}")
        return zip_path
    logger.info(f"Downloading {fname} ...")
    r = requests.get(url, timeout=300, verify=False)
    r.raise_for_status()
    with open(zip_path, "wb") as f:
        f.write(r.content)
    logger.info(f"Saved {fname} ({len(r.content)/1_000_000:.1f} MB)")
    return zip_path


def _ensure_shapefile(zip_filename: str, subdir: str, data_dir: Path):
    zip_path = data_dir / zip_filename
    if not zip_path.exists():
        raise FileNotFoundError(
            f"Missing {zip_filename}. Run with --download-data to fetch."
        )
    extract_dir = data_dir / subdir
    shp_exists = (
        any(p.suffix.lower() == ".shp" for p in extract_dir.glob("*.shp"))
        if extract_dir.exists()
        else False
    )
    if not shp_exists:
        logger.info(f"Extracting {zip_filename} ...")
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
    return extract_dir


def prepare_datasets(
    data_dir: Path, force_download: bool = False
) -> Tuple[Path, Path, Path]:
    data_dir.mkdir(parents=True, exist_ok=True)
    if force_download:
        logger.info("Force downloading TIGER datasets...")
        _download_zip(f"{TIGER_BASE_URL}/ZCTA520/{ZCTA_ZIP}", data_dir)
        _download_zip(f"{TIGER_BASE_URL}/COUNTY/{COUNTY_ZIP}", data_dir)
        _download_zip(f"{TIGER_BASE_URL}/STATE/{STATE_ZIP}", data_dir)
    else:
        # Download if missing
        for folder, fname in (
            ("ZCTA520", ZCTA_ZIP),
            ("COUNTY", COUNTY_ZIP),
            ("STATE", STATE_ZIP),
        ):
            url = f"{TIGER_BASE_URL}/{folder}/{fname}"
            if not (data_dir / fname).exists():
                _download_zip(url, data_dir)

    zcta_dir = _ensure_shapefile(ZCTA_ZIP, "zcta", data_dir)
    county_dir = _ensure_shapefile(COUNTY_ZIP, "county", data_dir)
    state_dir = _ensure_shapefile(STATE_ZIP, "state", data_dir)
    return zcta_dir, county_dir, state_dir


def load_geodata(zcta_dir: Path, county_dir: Path, state_dir: Path):
    # Find the .shp file in each directory
    def first_shp(d: Path) -> Path:
        for p in d.glob("*.shp"):
            return p
        raise FileNotFoundError(f"No .shp found in {d}")

    zcta_gdf = gpd.read_file(first_shp(zcta_dir))[["ZCTA5CE20", "geometry"]]
    county_gdf = gpd.read_file(first_shp(county_dir))[
        ["NAME", "STATEFP", "GEOID", "geometry"]
    ]
    state_gdf = gpd.read_file(first_shp(state_dir))[
        ["NAME", "STUSPS", "STATEFP", "geometry"]
    ]

    # Ensure all are in WGS84 (EPSG:4326)
    for g in (zcta_gdf, county_gdf, state_gdf):
        if g.crs is None:
            logger.warning(
                "A layer has no CRS; assuming EPSG:4269 (NAD83) -> converting to EPSG:4326"
            )
            g.set_crs(epsg=4269, inplace=True)
        g.to_crs(epsg=4326, inplace=True)

    # Build spatial indices (automatic on first sindex access)
    _ = zcta_gdf.sindex
    _ = county_gdf.sindex
    _ = state_gdf.sindex
    logger.info("Spatial indices prepared (ZCTA, County, State).")
    return zcta_gdf, county_gdf, state_gdf


def save_tiger_to_db(zcta_gdf, county_gdf, state_gdf, table_name="census_geodata"):
    """Save TIGER shapefile data to database table."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        logger.info(f"Creating table {table_name}...")
        cur.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{table_name} CASCADE")

        # Create table with geometry support
        cur.execute(
            f"""
            CREATE TABLE {DB_SCHEMA}.{table_name} (
                id SERIAL PRIMARY KEY,
                geoid VARCHAR(20),
                name VARCHAR(255),
                layer_type VARCHAR(20),  -- 'zcta', 'county', 'state'
                state_fips VARCHAR(2),
                geometry GEOMETRY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert ZCTA data
        logger.info("Inserting ZCTA data...")
        zcta_records = []
        for _, row in zcta_gdf.iterrows():
            zcta_records.append(
                (
                    row["ZCTA5CE20"],  # geoid
                    row["ZCTA5CE20"],  # name (same as geoid for ZCTA)
                    "zcta",
                    None,  # state_fips not in ZCTA layer
                    row["geometry"].wkt,  # Well-Known Text representation
                )
            )

        cur.executemany(
            f"""
            INSERT INTO {DB_SCHEMA}.{table_name} (geoid, name, layer_type, state_fips, geometry)
            VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326))
        """,
            zcta_records,
        )

        # Insert county data
        logger.info("Inserting county data...")
        county_records = []
        for _, row in county_gdf.iterrows():
            county_records.append(
                (
                    row["GEOID"],
                    row["NAME"],
                    "county",
                    row["STATEFP"],
                    row["geometry"].wkt,
                )
            )

        cur.executemany(
            f"""
            INSERT INTO {DB_SCHEMA}.{table_name} (geoid, name, layer_type, state_fips, geometry)
            VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326))
        """,
            county_records,
        )

        # Insert state data
        logger.info("Inserting state data...")
        state_records = []
        for _, row in state_gdf.iterrows():
            state_records.append(
                (
                    row["STATEFP"],
                    row["NAME"],
                    "state",
                    row["STATEFP"],
                    row["geometry"].wkt,
                )
            )

        cur.executemany(
            f"""
            INSERT INTO {DB_SCHEMA}.{table_name} (geoid, name, layer_type, state_fips, geometry)
            VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326))
        """,
            state_records,
        )

        conn.commit()

        total_records = len(zcta_records) + len(county_records) + len(state_records)
        logger.info(
            f"Successfully saved {total_records:,} TIGER records to {table_name}"
        )
        logger.info(
            f"ZCTA: {len(zcta_records):,}, County: {len(county_records):,}, State: {len(state_records):,}"
        )

        conn.close()
        return True

    except Exception as e:
        logger.error(f"Failed to save TIGER data to database: {e}")
        import traceback

        traceback.print_exc()
        return False


def spatial_join_points(points_df, zcta_gdf, county_gdf, state_gdf):
    """Perform spatial joins to enrich points with ZIP (ZCTA), county, and state.

    points_df columns: latitude, longitude
    Returns DataFrame with zip, county, state, state_fips
    """
    gdf_pts = gpd.GeoDataFrame(
        points_df,
        geometry=[
            Point(lon, lat) for lat, lon in zip(points_df.latitude, points_df.longitude)
        ],
        crs="EPSG:4326",
    )

    # Join order: state -> county -> zcta (reduces candidate sets logically for very large sets)
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
        county_gdf[["NAME", "STATEFP", "geometry"]],
        predicate="within",
        how="left",
        rsuffix="_county",
    )
    pts_county.rename(columns={"NAME": "county"}, inplace=True)
    pts_zcta = gpd.sjoin(
        pts_county, zcta_gdf, predicate="within", how="left", rsuffix="_zcta"
    )
    pts_zcta.rename(columns={"ZCTA5CE20": "zip"}, inplace=True)

    result = pts_zcta[
        ["latitude", "longitude", "zip", "county", "state", "state_fips"]
    ].copy()
    result["zip"].fillna("", inplace=True)
    result["county"].fillna("", inplace=True)
    result["state"].fillna("", inplace=True)
    result["state_fips"].fillna("", inplace=True)
    return result


def geocode_coordinates_to_location_data(
    table_name="location_data",
    data_dir: Optional[str] = None,
    force_download: bool = False,
):
    """Geocode using TIGER/Line shapefiles (ZCTA, County, State)."""
    try:
        data_path = _get_data_dir(data_dir)
        zcta_dir, county_dir, state_dir = prepare_datasets(
            data_path, force_download=force_download
        )
        zcta_gdf, county_gdf, state_gdf = load_geodata(zcta_dir, county_dir, state_dir)

        # Save TIGER data to database for future spatial queries
        logger.info("Saving TIGER/Line geodata to census_geodata table...")
        if not save_tiger_to_db(
            zcta_gdf, county_gdf, state_gdf, table_name="census_geodata"
        ):
            logger.warning(
                "Failed to save TIGER data to database, but continuing with geocoding..."
            )

        conn = get_db_connection()
        cur = conn.cursor()
        logger.info("Fetching distinct valid coordinates from source table ...")
        cur.execute(
            f"""
            SELECT DISTINCT
                CAST(latitude AS DOUBLE PRECISION) AS latitude,
                CAST(longitude AS DOUBLE PRECISION) AS longitude
            FROM {SOURCE_TABLE}
            WHERE {COORD_PREDICATE}
            """
        )
        rows = cur.fetchall()
        if not rows:
            logger.warning("No coordinates found to process.")
            return False
        coords_df = pd.DataFrame(rows, columns=["latitude", "longitude"])
        logger.info(f"Loaded {len(coords_df):,} unique coordinate pairs.")

        start = time.time()
        enriched = spatial_join_points(coords_df, zcta_gdf, county_gdf, state_gdf)
        elapsed = time.time() - start
        logger.info(
            f"Spatial joins complete in {elapsed:.1f}s. ZIP coverage: {(enriched['zip']!='').mean()*100:.1f}%"
        )

        # Replace table
        logger.info("Writing results to database ...")
        cur.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{table_name} CASCADE")
        cur.execute(
            f"""
            CREATE TABLE {DB_SCHEMA}.{table_name} (
                id SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                zip VARCHAR(10),
                county VARCHAR(100),
                state VARCHAR(100),
                state_fips VARCHAR(2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Bulk insert
        records = list(enriched.itertuples(index=False, name=None))
        cur.executemany(
            f"INSERT INTO {DB_SCHEMA}.{table_name} (latitude, longitude, zip, county, state, state_fips) VALUES (%s,%s,%s,%s,%s,%s)",
            records,
        )
        conn.commit()

        # Indexes for query performance
        try:
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_lat_lon ON {DB_SCHEMA}.{table_name}(latitude, longitude);"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_zip ON {DB_SCHEMA}.{table_name}(zip);"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {DB_SCHEMA}.{table_name}(state);"
            )
            conn.commit()
            logger.info("Added indexes on latitude/longitude, zip, state")
        except Exception as ie:
            logger.warning(f"Failed to create one or more indexes: {ie}")

        logger.info(f"Inserted {len(records):,} rows into {table_name}.")
        zip_populated = (enriched["zip"] != "").sum()
        zip_pct = (enriched["zip"] != "").mean() * 100
        logger.info(
            "ZIP codes populated: %s (%.1f%%)",
            f"{zip_populated:,}",
            zip_pct,
        )
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Geocoding failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main(args):
    logger.info("Initializing TIGER/Line geocoding pipeline ...")
    if args.test_only:
        return test_database_connection()
    if not test_database_connection():
        logger.error("Database prerequisite check failed.")
        return False
    start = time.time()
    ok = geocode_coordinates_to_location_data(
        table_name=args.table_name,
        data_dir=args.data_dir,
        force_download=args.download_data,
    )
    if ok:
        logger.info(f"Done in {time.time()-start:.1f}s")
    return ok


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Offline TIGER/Line geocoder (ZIP/County/State)"
    )
    parser.add_argument(
        "--table-name", default="location_data", help="Destination table name"
    )
    parser.add_argument(
        "--data-dir", default=None, help="Directory to store/download TIGER shapefiles"
    )
    parser.add_argument(
        "--download-data", action="store_true", help="Force download TIGER datasets"
    )
    parser.add_argument(
        "--test-only", action="store_true", help="Only test DB connectivity"
    )
    args = parser.parse_args()

    success = main(args)
    if not success:
        exit(1)
