#!/usr/bin/env python
"""
export_to_convex.py — Export EduInsight data from Neon to Convex NDJSON.

Pulls dev.golden_table (math scores + income %), joins to the school directory
for lat/lon/state/county, aggregates to county and state level, computes
Pearson r, and writes NDJSON files ready for `npx convex import`.

Usage:
    python scripts/export_to_convex.py [--out-dir scripts/convex_export]

Output files (in --out-dir):
    districts.jsonl    — one record per school (~23K rows)
    county_stats.jsonl — one record per county
    state_stats.jsonl  — one record per state

Then import with:
    cd web
    npx convex import --table districts   ../scripts/convex_export/districts.jsonl
    npx convex import --table county_stats ../scripts/convex_export/county_stats.jsonl
    npx convex import --table state_stats  ../scripts/convex_export/state_stats.jsonl
"""

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    sys.exit("psycopg2 not found — run: pip install psycopg2-binary")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # .env optional if vars are already in environment

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# Join golden_table to the best available directory year (prefers most recent)
# for geographic fields (state, county_fips, zip, lat, lon).
# golden_table was materialized from 2021 data; we recover geography from
# the closest available year via case-insensitive school_name match.
EXPORT_QUERY = """
WITH dir_geo AS (
    SELECT DISTINCT ON (UPPER(school_name))
        UPPER(school_name)   AS sn,
        ncessch,
        state_location       AS state,
        county_code          AS county_fips,
        zip_location         AS zip,
        latitude::float      AS lat,
        longitude::float     AS lon
    FROM test.urban_ccd_directory_exp
    ORDER BY UPPER(school_name), year_json DESC
)
SELECT
    g.school_name,
    d.ncessch,
    d.state,
    d.county_fips,
    d.zip,
    d.lat,
    d.lon,
    g.math_high_pct,
    g.math_low_pct,
    CASE
        WHEN g.math_high_pct IS NOT NULL AND g.math_low_pct IS NOT NULL
            THEN (g.math_high_pct::float + g.math_low_pct::float) / 2.0
        ELSE COALESCE(g.math_high_pct::float, g.math_low_pct::float)
    END                         AS math_pct_prof,
    COALESCE(g.pct_hhi_150k_200k, 0)::float
        + COALESCE(g.pct_hhi_220k_plus, 0)::float
                                AS pct_high_income,
    g.pct_hhi_150k_200k::float  AS pct_hhi_150k_200k,
    g.pct_hhi_220k_plus::float  AS pct_hhi_220k_plus,
    g.enrollment
FROM dev.golden_table g
LEFT JOIN dir_geo d ON d.sn = UPPER(g.school_name)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def pearson_r(xs: list, ys: list):
    """Return Pearson r rounded to 4 dp, or None if n < 3 or variance is 0."""
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = math.sqrt(
        sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys)
    )
    if den == 0:
        return None
    return round(num / den, 4)


def safe_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def write_ndjson(path: Path, records: list):
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps({k: v for k, v in rec.items() if v is not None}) + "\n")
    print(f"  wrote {len(records):>6,} records → {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", default="scripts/convex_export",
                        help="Directory for output NDJSON files (default: scripts/convex_export)")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Connect ---
    conn_params = dict(
        host=os.getenv("NEON_HOST"),
        port=int(os.getenv("NEON_PORT", 5432)),
        dbname=os.getenv("NEON_NAME"),
        user=os.getenv("NEON_USER"),
        password=os.getenv("NEON_PASSWORD"),
        sslmode=os.getenv("NEON_SSLMODE", "require"),
    )
    missing = [k for k, v in conn_params.items() if not v]
    if missing:
        sys.exit(f"Missing env vars: {missing}. Set them or add to .env")

    print("Connecting to Neon…")
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    print("Fetching data from dev.golden_table…")
    cur.execute(EXPORT_QUERY)
    rows = cur.fetchall()
    conn.close()
    print(f"Fetched {len(rows):,} rows")

    # --- Build collections ---
    districts = []
    county_buckets: dict = defaultdict(lambda: {"math": [], "income": [], "state": None, "county_fips": None})
    state_buckets: dict = defaultdict(lambda: {"math": [], "income": []})

    geo_matched = 0

    for row in rows:
        state       = row["state"]
        county_fips = row["county_fips"]
        math_val    = safe_float(row["math_pct_prof"])
        income_val  = safe_float(row["pct_high_income"])

        if state:
            geo_matched += 1

        district = {
            "school_name":       row["school_name"],
            "ncessch":           row["ncessch"],
            "state":             state,
            "county_fips":       county_fips,
            "zip":               row["zip"],
            "lat":               safe_float(row["lat"]),
            "lon":               safe_float(row["lon"]),
            "math_pct_prof":     math_val,
            "pct_high_income":   income_val,
            "pct_hhi_150k_200k": safe_float(row["pct_hhi_150k_200k"]),
            "pct_hhi_220k_plus": safe_float(row["pct_hhi_220k_plus"]),
            "enrollment":        row["enrollment"],
        }
        districts.append(district)

        if state and county_fips and math_val is not None and income_val is not None:
            key = f"{state}_{county_fips}"
            county_buckets[key]["math"].append(math_val)
            county_buckets[key]["income"].append(income_val)
            county_buckets[key]["state"] = state
            county_buckets[key]["county_fips"] = county_fips

        if state and math_val is not None and income_val is not None:
            state_buckets[state]["math"].append(math_val)
            state_buckets[state]["income"].append(income_val)

    pct_geo = geo_matched / len(rows) * 100 if rows else 0
    print(f"Geography matched: {geo_matched:,} / {len(rows):,} ({pct_geo:.1f}%)")

    # --- Aggregate county_stats ---
    county_stats = []
    for data in county_buckets.values():
        xs, ys = data["income"], data["math"]
        county_stats.append({
            "state":              data["state"],
            "county_fips":        data["county_fips"],
            "avg_math_pct_prof":  round(sum(ys) / len(ys), 2),
            "avg_pct_high_income": round(sum(xs) / len(xs), 2),
            "pearson_r":          pearson_r(xs, ys),
            "school_count":       len(ys),
        })

    # --- Aggregate state_stats ---
    state_stats = []
    for state, data in state_buckets.items():
        xs, ys = data["income"], data["math"]
        state_stats.append({
            "state":              state,
            "avg_math_pct_prof":  round(sum(ys) / len(ys), 2),
            "avg_pct_high_income": round(sum(xs) / len(xs), 2),
            "pearson_r":          pearson_r(xs, ys),
            "school_count":       len(ys),
        })

    # --- Write NDJSON ---
    print("\nWriting NDJSON…")
    write_ndjson(out_dir / "districts.jsonl",   districts)
    write_ndjson(out_dir / "county_stats.jsonl", county_stats)
    write_ndjson(out_dir / "state_stats.jsonl",  state_stats)

    print(f"\nAll done. Files in: {out_dir.resolve()}")
    print("\nNext — import into Convex:")
    print("  cd web")
    print("  npx convex import --table districts    ../scripts/convex_export/districts.jsonl")
    print("  npx convex import --table county_stats  ../scripts/convex_export/county_stats.jsonl")
    print("  npx convex import --table state_stats   ../scripts/convex_export/state_stats.jsonl")


if __name__ == "__main__":
    main()
