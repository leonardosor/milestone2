#!/usr/bin/env python3
import json
import logging
import os
import sys
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

# Import ConfigLoader
sys.path.append(str(Path(__file__).parent.parent / "config"))
from config_loader import ConfigLoader

load_dotenv(override=True)

# Ensure logs directory exists
os.makedirs("/app/logs", exist_ok=True)

def _strip_fips(series):
    """Strip leading zeros from FIPS codes, replace empty with '0'."""
    return series.astype(str).str.lstrip("0").replace({"": "0"})

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/location_etl.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

DB_SCHEMA = None
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
    """Load configuration using ConfigLoader"""
    global DB_SCHEMA
    config_loader = ConfigLoader(config_file)
    DB_SCHEMA = config_loader.config.get("schema", "public")
    logger.info(f"Configuration loaded with schema: {DB_SCHEMA}")
    return config_loader.config

def get_db_connection(config_file="config.json"):
    """Get database connection using ConfigLoader"""
    config_loader = ConfigLoader(config_file)
    global DB_SCHEMA
    DB_SCHEMA = config_loader.config.get("schema", "public")

    # Use ConfigLoader's psycopg2 connection params
    conn_params = config_loader.get_psycopg2_connection_params()
    conn = psycopg2.connect(**conn_params)

    if DB_SCHEMA:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA};")
        conn.commit()
        logger.info(f"Schema '{DB_SCHEMA}' is ready")

    return conn

def test_database_connection():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
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
                return count > 0

    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False

def _get_data_dir(cli_dir: Optional[str] = None) -> Path:
    return Path(cli_dir or os.getenv("TIGER_DATA_DIR") or "tiger_data").resolve()

def _download_file(url: str, target_dir: Path) -> Path:
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
    if list(extract_dir.glob("*.shp")):
        return

    logger.info(f"Extracting {zip_path.name} ...")
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

def prepare_datasets(data_dir: Path, force_download: bool = False) -> Tuple[Path, Path, Path]:
    data_dir.mkdir(parents=True, exist_ok=True)

    if force_download:
        logger.info("Force downloading TIGER datasets...")

    for layer, (zip_name, folder) in TIGER_FILES.items():
        if force_download or not (data_dir / zip_name).exists():
            _download_file(f"{TIGER_BASE_URL}/{folder}/{zip_name}", data_dir)

    dirs = {layer: data_dir / layer for layer in TIGER_FILES}
    for layer, (zip_name, _) in TIGER_FILES.items():
        _extract_shapefile(data_dir / zip_name, dirs[layer])

    return dirs["zcta"], dirs["county"], dirs["state"]

def load_geodata(zcta_dir: Path, county_dir: Path, state_dir: Path):
    def find_shp_file(directory: Path) -> Path:
        return next(directory.glob("*.shp"), None) or (_ for _ in ()).throw(
            FileNotFoundError(f"No .shp file found in {directory}")
        )

    zcta_gdf = gpd.read_file(find_shp_file(zcta_dir))[["ZCTA5CE20", "geometry"]]
    county_gdf = gpd.read_file(find_shp_file(county_dir))[["NAME", "STATEFP", "GEOID", "COUNTYFP", "geometry"]]
    state_gdf = gpd.read_file(find_shp_file(state_dir))[["NAME", "STUSPS", "STATEFP", "geometry"]]

    for gdf in [state_gdf, county_gdf]:
        gdf["STATEFP"] = _strip_fips(gdf["STATEFP"])
    county_gdf["COUNTYFP"] = _strip_fips(county_gdf["COUNTYFP"])

    for gdf in [zcta_gdf, county_gdf, state_gdf]:
        if gdf.crs is None:
            logger.warning("A layer has no CRS; assuming EPSG:4269 -> converting to EPSG:4326")
            gdf.set_crs(epsg=4269, inplace=True)
        gdf.to_crs(epsg=4326, inplace=True)
        _ = gdf.sindex

    logger.info("Spatial indices prepared (ZCTA, County, State)")
    return zcta_gdf, county_gdf, state_gdf

def save_tiger_to_db(zcta_gdf, county_gdf, state_gdf, table_name="census_geodata"):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                logger.info(f"Creating table {table_name}...")
                cur.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{table_name} CASCADE")
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

                records = [
                    (row["ZCTA5CE20"], row["ZCTA5CE20"], "zcta", None, None, row["geometry"].wkt)
                    for _, row in zcta_gdf.iterrows()
                ] + [
                    (row["GEOID"], row["NAME"], "county", row["STATEFP"], row["COUNTYFP"], row["geometry"].wkt)
                    for _, row in county_gdf.iterrows()
                ] + [
                    (row["STATEFP"], row["NAME"], "state", row["STATEFP"], None, row["geometry"].wkt)
                    for _, row in state_gdf.iterrows()
                ]

                cur.executemany(
                    f"""
                    INSERT INTO {DB_SCHEMA}.{table_name}
                    (geoid, name, layer_type, state_fips, county_fips, geometry)
                    VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
                """,
                    records,
                )

            conn.commit()
            logger.info(f"Successfully saved {len(records):,} TIGER records to {table_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to save TIGER data to database: {e}")
        return False

def spatial_join_points(points_df, zcta_gdf, county_gdf, state_gdf):
    gdf_pts = gpd.GeoDataFrame(
        points_df,
        geometry=[Point(lon, lat) for lat, lon in zip(points_df.latitude, points_df.longitude)],
        crs="EPSG:4326",
    )

    result = gdf_pts
    for gdf, cols, rename_map, suffix in [
        (state_gdf, ["NAME", "STATEFP", "geometry"], {"NAME": "state", "STATEFP": "state_fips"}, "_state"),
        (county_gdf, ["NAME", "STATEFP", "COUNTYFP", "geometry"], {"NAME": "county", "COUNTYFP": "county_fips"}, "_county"),
        (zcta_gdf, None, {"ZCTA5CE20": "zip"}, "_zcta"),
    ]:
        result = gpd.sjoin(
            result, gdf[cols] if cols else gdf, predicate="within", how="left", rsuffix=suffix
        )
        result.rename(columns=rename_map, inplace=True)

    result = result[["latitude", "longitude", "zip", "county", "county_fips", "state", "state_fips"]].copy()
    result["state_fips"] = _strip_fips(result["state_fips"])
    result["county_fips"] = _strip_fips(result["county_fips"])
    result.fillna("", inplace=True)
    return result

def geocode_coordinates_to_location_data(
    table_name="location_data",
    data_dir: Optional[str] = None,
    force_download: bool = False,
):
    try:
        data_path = _get_data_dir(data_dir)
        zcta_dir, county_dir, state_dir = prepare_datasets(data_path, force_download)
        zcta_gdf, county_gdf, state_gdf = load_geodata(zcta_dir, county_dir, state_dir)

        logger.info("Saving TIGER/Line geodata to census_geodata table...")
        if not save_tiger_to_db(zcta_gdf, county_gdf, state_gdf, table_name="census_geodata"):
            logger.warning("Failed to save TIGER data, continuing with geocoding...")

        with get_db_connection() as conn:
            with conn.cursor() as cur:
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

                start = time.time()
                enriched = spatial_join_points(coords_df, zcta_gdf, county_gdf, state_gdf)
                zip_coverage = (enriched["zip"] != "").mean() * 100
                logger.info(f"Spatial joins completed in {time.time()-start:.1f}s. ZIP coverage: {zip_coverage:.1f}%")

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

            with conn.cursor() as cur:
                for col_spec in ["lat_lon ON (latitude, longitude)", "zip ON (zip)", "state ON (state)", "county_fips ON (county_fips)"]:
                    idx_name, col_def = col_spec.split(" ON ")
                    try:
                        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{idx_name} ON {DB_SCHEMA}.{table_name}{col_def}")
                    except Exception as ie:
                        logger.warning(f"Failed to create index idx_{table_name}_{idx_name}: {ie}")

            conn.commit()
            zip_count = (enriched["zip"] != "").sum()
            logger.info(f"Inserted {len(records):,} rows. ZIP codes populated: {zip_count:,} ({zip_count/len(records)*100:.1f}%)")

        return True

    except Exception as e:
        logger.error(f"Geocoding failed: {e}")
        return False

def main(args):
    logger.info("Starting TIGER/Line geocoding pipeline")

    if args.test_only or not test_database_connection():
        return test_database_connection() if args.test_only else (logger.error("Database prerequisite check failed") or False)

    start = time.time()
    success = geocode_coordinates_to_location_data(
        table_name=args.table_name, data_dir=args.data_dir, force_download=args.download_data
    )

    if success:
        logger.info(f"Pipeline completed in {time.time()-start:.1f}s")
    return success

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Offline TIGER/Line geocoder")
    parser.add_argument("--table-name", default="location_data", help="Destination table name")
    parser.add_argument("--data-dir", help="Directory for TIGER shapefiles")
    parser.add_argument("--download-data", action="store_true", help="Force download TIGER datasets")
    parser.add_argument("--test-only", action="store_true", help="Test DB connectivity only")

    args = parser.parse_args()
    success = main(args)
    exit(0 if success else 1)
