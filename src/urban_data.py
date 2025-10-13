"""Urban Institute ETL"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
from contextlib import nullcontext
from datetime import datetime
from typing import Dict, List

import aiohttp
import backoff
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

try:
    from wakepy import keep  # type: ignore
except ImportError:
    logger.error("wakepy is required. Install it with: pip install wakepy")
    sys.exit(1)

DB_SCHEMA = None


def load_config(config_file: str) -> Dict:
    global DB_SCHEMA
    search = []
    if os.path.isabs(config_file):
        search.append(config_file)
    else:
        cwd = os.getcwd()
        script_dir = os.path.dirname(os.path.abspath(__file__))
        search.extend(
            [
                os.path.join(cwd, config_file),
                os.path.join(script_dir, config_file),
                os.path.join(os.path.dirname(script_dir), config_file),
            ]
        )
    last_err = None
    for p in search:
        try:
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                DB_SCHEMA = cfg.get("schema")
                if not DB_SCHEMA:
                    raise ValueError("Missing 'schema' in config.json")
                logger.info(f"Loaded config from {p}")
                return cfg
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {p}: {e}")
            raise
        except Exception as e:
            last_err = e
    logger.error("Configuration file not found. Tried: " + ", ".join(search))
    if last_err:
        raise last_err
    raise FileNotFoundError(config_file)


def sanitize_identifier(name: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
    while "__" in safe:
        safe = safe.replace("__", "_")
    if safe and safe[0].isdigit():
        safe = f"t_{safe}"
    return safe.lower()


class EndpointTableManager:
    def __init__(self, engine, drop_existing: bool = False):
        self.engine = engine
        self._created: set[str] = set()
        self._drop_existing = drop_existing

    def ensure_schema(self):
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA};"))
            conn.commit()

    def ensure_table(self, endpoint_key: str, table_name: str | None = None):
        if endpoint_key in self._created:
            return
        table = table_name or f"urban_{sanitize_identifier(endpoint_key)}"
        drop_sql = (
            f"DROP TABLE IF EXISTS {DB_SCHEMA}.{table};" if self._drop_existing else ""
        )
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{table} (
            id SERIAL PRIMARY KEY,
            year INTEGER NOT NULL,
            data_json JSONB NOT NULL,
            data_hash VARCHAR(64) UNIQUE,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_{table}_year ON {DB_SCHEMA}.{table}(year);
        CREATE INDEX IF NOT EXISTS idx_{table}_json ON {DB_SCHEMA}.{table} USING GIN(data_json);
        CREATE INDEX IF NOT EXISTS idx_{table}_hash ON {DB_SCHEMA}.{table}(data_hash);
        """
        with self.engine.connect() as conn:
            if drop_sql:
                conn.execute(text(drop_sql))
            conn.execute(text(create_sql))
            conn.commit()
        logger.info(f"Table ready: {DB_SCHEMA}.{table}")
        self._created.add(endpoint_key)

    def bulk_insert(
        self, endpoint_key: str, records: List[dict], table_name: str | None = None
    ):
        if not records:
            return 0
        table = table_name or f"urban_{sanitize_identifier(endpoint_key)}"
        insert_sql = text(
            f"""
            INSERT INTO {DB_SCHEMA}.{table} (year, data_json, data_hash, fetched_at)
            VALUES (:year, CAST(:data_json AS JSONB), :data_hash, :fetched_at)
            ON CONFLICT (data_hash) DO NOTHING
        """
        )
        with self.engine.connect() as conn:
            result = conn.execute(insert_sql, records)
            conn.commit()
        return result.rowcount if hasattr(result, "rowcount") else 0


class EndpointETL:
    def __init__(self, config: Dict, drop_existing: bool = False):
        self.config = config
        db = config.get("local_database", {})
        conn_str = f"postgresql://{db['username']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
        self.engine = create_engine(
            conn_str,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        self.tables = EndpointTableManager(self.engine, drop_existing=drop_existing)
        self.tables.ensure_schema()
        self.urban_cfg = self.config.get("urban", {})
        self.endpoint_templates: Dict[str, str] = self.urban_cfg.get("endpoints", {})
        self.raw_table_names: Dict[str, str] = {}
        self._assign_table_names()

    def _assign_table_names(self):
        used: set[str] = set()
        for key, template in self.endpoint_templates.items():
            name = self._derive_table_name_from_template(template, key)
            base = name
            i = 1
            while name in used:
                name = f"{base}_{i}"
                i += 1
            used.add(name)
            self.raw_table_names[key] = name

    @staticmethod
    def _derive_table_name_from_template(template: str, fallback_key: str) -> str:
        segs = [s for s in template.strip("/").split("/") if s]
        filtered: list[str] = []
        for s in segs:
            low = s.lower()
            if low in {"api", "v1", "schools"}:
                continue
            if low.startswith("{") and low.endswith("}"):
                continue
            filtered.append(low.replace("-", "_"))
        if not filtered:
            filtered = [sanitize_identifier(fallback_key)]
        if len(filtered) > 5:
            filtered = filtered[-5:]
        candidate = "urban_" + "_".join(filtered)
        if len(candidate) > 55:
            short_parts = []
            for part in filtered:
                if len(part) > 12:
                    short_parts.append(part[:8])
                else:
                    short_parts.append(part)
            candidate = "urban_" + "_".join(short_parts)
        if len(candidate) > 60:
            candidate = candidate[:60]
        return sanitize_identifier(candidate)

    @staticmethod
    def _giveup(e):
        return (
            isinstance(e, aiohttp.ClientResponseError)
            and 400 <= e.status < 500
            and e.status not in (429,)
        )

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ClientResponseError),
        max_tries=5,
        giveup=_giveup,
        jitter=backoff.full_jitter,
    )
    async def _fetch_page(self, session: aiohttp.ClientSession, url: str):
        async with session.get(url) as resp:
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=resp.status,
                    message=f"Status {resp.status}",
                )
            return await resp.json()

    async def _fetch_all(
        self,
        session,
        base_url: str,
        endpoint_template: str,
        year: int,
        page_delay: float,
        max_pages: int | None,
    ) -> list:
        ep = endpoint_template.format(year=year)
        url = f"{base_url}{ep}"
        results = []
        page = 0
        next_url = url
        while next_url and (max_pages is None or page < max_pages):
            page += 1
            try:
                data = await self._fetch_page(session, next_url)
            except Exception as e:
                logger.error(f"Failed {ep} page {page}: {e}")
                break
            page_results = data.get("results", [])
            results.extend(page_results)
            if page == 1:
                logger.info(f"{ep} {year}: page {page} -> {len(page_results)} records")
            else:
                logger.debug(
                    f"{ep} {year}: page {page} -> {len(page_results)} (cumulative {len(results)})"
                )
            nxt = data.get("next")
            if nxt:
                next_url = (
                    nxt
                    if nxt.startswith("http")
                    else f"{base_url.rstrip('/')}/{nxt.lstrip('/')}"
                )
                await asyncio.sleep(page_delay)
            else:
                next_url = None
        return results

    async def ingest(
        self,
        begin_year: int,
        end_year: int,
        endpoint_subset: List[str] | None,
        max_concurrency: int,
        page_delay: float,
        flush_threshold: int,
    ) -> Dict:
        urban_cfg = self.urban_cfg
        base_url = urban_cfg.get("base_url", "")
        endpoints_map: Dict[str, str] = self.endpoint_templates.copy()
        if endpoint_subset:
            missing = [e for e in endpoint_subset if e not in endpoints_map]
            if missing:
                raise ValueError(f"Endpoint keys not found in config: {missing}")
            endpoints_map = {
                k: v for k, v in endpoints_map.items() if k in endpoint_subset
            }
            self.raw_table_names = {
                k: v for k, v in self.raw_table_names.items() if k in endpoints_map
            }

        pagination_cfg = urban_cfg.get("pagination", {})
        max_pages = pagination_cfg.get("max_pages_per_endpoint")

        semaphore = asyncio.Semaphore(max_concurrency)
        queue: asyncio.Queue = asyncio.Queue(maxsize=flush_threshold * 2)
        total_inserted = 0
        total_seen = 0

        try:
            import orjson  # type: ignore

            def dumps(obj):
                return orjson.dumps(obj).decode()

        except Exception:

            def dumps(obj):
                return json.dumps(obj)

        async def writer():
            nonlocal total_inserted
            buffer_per_endpoint: Dict[str, list] = {}
            while True:
                item = await queue.get()
                if item is None:
                    break
                ep_key = item["endpoint_key"]
                buffer_per_endpoint.setdefault(ep_key, []).append(item)
                if len(buffer_per_endpoint[ep_key]) >= flush_threshold:
                    self.tables.ensure_table(ep_key, self.raw_table_names[ep_key])
                    inserted = self.tables.bulk_insert(
                        ep_key,
                        [
                            {
                                "year": r["year"],
                                "data_json": r["data_json"],
                                "data_hash": r["data_hash"],
                                "fetched_at": r["fetched_at"],
                            }
                            for r in buffer_per_endpoint[ep_key]
                        ],
                        table_name=self.raw_table_names[ep_key],
                    )
                    total_inserted += inserted
                    buffer_per_endpoint[ep_key].clear()
            for ep_key, buf in buffer_per_endpoint.items():
                if buf:
                    self.tables.ensure_table(ep_key, self.raw_table_names[ep_key])
                    inserted = self.tables.bulk_insert(
                        ep_key,
                        [
                            {
                                "year": r["year"],
                                "data_json": r["data_json"],
                                "data_hash": r["data_hash"],
                                "fetched_at": r["fetched_at"],
                            }
                            for r in buf
                        ],
                        table_name=self.raw_table_names[ep_key],
                    )
                    total_inserted += inserted
            logger.info(f"Writer finished. Total inserted (unique): {total_inserted}")

        async def process(
            ep_key: str, template: str, year: int, session: aiohttp.ClientSession
        ):
            nonlocal total_seen
            async with semaphore:
                records = await self._fetch_all(
                    session, base_url, template, year, page_delay, max_pages
                )
            if not records:
                return
            now = datetime.utcnow()
            for rec in records:
                json_text = dumps(rec)
                data_hash = hashlib.sha256(
                    f"{ep_key}_{year}_{json_text}".encode("utf-8")
                ).hexdigest()
                await queue.put(
                    {
                        "endpoint_key": ep_key,
                        "year": year,
                        "data_json": json_text,
                        "data_hash": data_hash,
                        "fetched_at": now,
                    }
                )
            total_seen += len(records)
            logger.info(
                f"Queued {len(records)} rows for {ep_key} {year} (cumulative seen {total_seen})"
            )

        timeout = aiohttp.ClientTimeout(total=None)
        headers = {
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "UrbanEndpointETL/1.0",
        }
        writer_task = asyncio.create_task(writer())
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            tasks = []
            for year in range(begin_year, end_year + 1):
                for ep_key, template in endpoints_map.items():
                    tasks.append(
                        asyncio.create_task(process(ep_key, template, year, session))
                    )
            await asyncio.gather(*tasks)
        await queue.put(None)
        await writer_task
        stats = {
            "rows_seen": total_seen,
            "rows_inserted": total_inserted,
            "endpoint_tables": [
                f"{DB_SCHEMA}.{self.raw_table_names[k]}" for k in endpoints_map.keys()
            ],
            "endpoint_keys": list(endpoints_map.keys()),
        }
        return stats

    def build_per_endpoint_expanded_tables(
        self,
        endpoint_keys: List[str],
        suffix: str = "expanded",
        drop_existing: bool = True,
    ):
        results = []
        with self.engine.connect() as conn:
            for ep_key in endpoint_keys:
                raw_table = self.raw_table_names.get(
                    ep_key, f"urban_{sanitize_identifier(ep_key)}"
                )
                expanded_table = f"{raw_table}_{sanitize_identifier(suffix)}"
                full_expanded = f"{DB_SCHEMA}.{expanded_table}"
                logger.info(f"Expanding endpoint '{ep_key}' into {full_expanded}")
                try:
                    key_rows = conn.execute(
                        text(
                            f"SELECT DISTINCT jsonb_object_keys(data_json) AS k FROM {DB_SCHEMA}.{raw_table}"
                        )
                    )
                    key_rows = key_rows.fetchall()
                except Exception as e:
                    logger.warning(f"Skipping expansion for {raw_table}: {e}")
                    continue
                used_cols: set[str] = set()
                key_map: Dict[str, str] = {}
                reserved = {"year", "fetched_at", "id"}
                for (orig_key,) in key_rows:
                    if orig_key is None:
                        continue
                    base = sanitize_identifier(str(orig_key))
                    if base in reserved:
                        base = f"{base}_json"
                    col = base
                    i = 1
                    while col in used_cols:
                        col = f"{base}_{i}"
                        i += 1
                    used_cols.add(col)
                    key_map[orig_key] = col
                if drop_existing:
                    conn.execute(text(f"DROP TABLE IF EXISTS {full_expanded} CASCADE;"))
                col_defs = [
                    "id SERIAL PRIMARY KEY",
                    "year INTEGER",
                    "fetched_at TIMESTAMP",
                ] + [f'"{v}" TEXT' for v in sorted(key_map.values())]
                ddl = f"CREATE TABLE {full_expanded} (" + ",".join(col_defs) + ");"
                conn.execute(text(ddl))
                conn.execute(
                    text(
                        f"CREATE INDEX idx_{expanded_table}_year ON {full_expanded}(year);"
                    )
                )
                conn.commit()
                if key_map:
                    select_cols = []
                    for orig, col in key_map.items():
                        safe_key = str(orig).replace("'", "''")
                        select_cols.append(f"data_json ->> '{safe_key}' AS \"{col}\"")
                    select_list = ",".join(select_cols)
                    insert_sql = f"""
                    INSERT INTO {full_expanded} (year, fetched_at, {','.join(f'"{c}"' for c in key_map.values())})
                    SELECT year, fetched_at, {select_list}
                    FROM {DB_SCHEMA}.{raw_table};
                    """
                    try:
                        conn.execute(text(insert_sql))
                        conn.commit()
                    except Exception as e:
                        logger.warning(f"Insert failed for {full_expanded}: {e}")
                count_res = conn.execute(
                    text(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{raw_table}")
                )
                raw_count = count_res.scalar() or 0
                results.append(
                    {
                        "endpoint_key": ep_key,
                        "raw_table": f"{DB_SCHEMA}.{raw_table}",
                        "expanded_table": full_expanded,
                        "column_count": len(key_map),
                        "raw_row_count": raw_count,
                    }
                )
                logger.info(
                    f"Created expanded table {full_expanded} with {len(key_map)} JSON-derived columns"
                )
        return results


async def main():
    parser = argparse.ArgumentParser(
        description="Urban Institute ETL (one raw + one expanded table per endpoint)"
    )
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    parser.add_argument(
        "--begin-year", type=int, required=True, help="Start year (inclusive)"
    )
    parser.add_argument(
        "--end-year", type=int, required=True, help="End year (inclusive)"
    )
    parser.add_argument(
        "--endpoints", type=str, help="Comma-separated endpoint keys (subset)"
    )
    parser.add_argument(
        "--max-concurrency", type=int, help="Override max concurrent requests"
    )
    parser.add_argument(
        "--batch-size", type=int, help="Flush threshold per endpoint buffer"
    )
    parser.add_argument(
        "--page-delay-ms", type=int, help="Delay between paginated requests (ms)"
    )
    parser.add_argument(
        "--keep-awake", action="store_true", help="Keep system awake during run"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing per-endpoint tables before ingest",
    )
    parser.add_argument(
        "--skip-expand",
        action="store_true",
        help="Skip per-endpoint expanded tables creation",
    )
    parser.add_argument(
        "--expanded-suffix",
        default="expanded",
        help="Suffix for per-endpoint expanded tables (default: expanded)",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.begin_year > args.end_year:
        logger.error("begin-year cannot be greater than end-year")
        sys.exit(1)

    cfg = load_config(args.config)
    async_cfg = cfg.get("async", {})
    etl_cfg = cfg.get("etl", {})
    urb_pag = cfg.get("urban", {}).get("pagination", {})
    max_concurrency = args.max_concurrency or async_cfg.get(
        "max_concurrent_requests", 10
    )
    batch_size = args.batch_size or etl_cfg.get("batch_size", 1000)
    page_delay = (
        (args.page_delay_ms / 1000.0)
        if args.page_delay_ms
        else urb_pag.get("page_delay_ms", 300) / 1000.0
    )

    subset = None
    if args.endpoints:
        subset = [e.strip() for e in args.endpoints.split(",") if e.strip()]

    etl = EndpointETL(cfg, drop_existing=args.drop_existing)
    wake_ctx = keep.running() if args.keep_awake else None
    try:
        if wake_ctx:
            logger.info("wakepy engaged; preventing sleep...")
        with wake_ctx or nullcontext():
            start = datetime.utcnow()
            stats = await etl.ingest(
                begin_year=args.begin_year,
                end_year=args.end_year,
                endpoint_subset=subset,
                max_concurrency=max_concurrency,
                page_delay=page_delay,
                flush_threshold=batch_size,
            )
        elapsed = datetime.utcnow() - start
        logger.info("=" * 72)
        logger.info("ETL COMPLETE")
        logger.info(
            f"Rows seen: {stats['rows_seen']} | Rows inserted (unique): {stats['rows_inserted']}"
        )
        logger.info("Tables:")
        for t in stats["endpoint_tables"]:
            logger.info(f"  - {t}")
        logger.info(f"Duration: {elapsed}")
        if not args.skip_expand:
            logger.info("Beginning per-endpoint expansion phase...")
            expansions = etl.build_per_endpoint_expanded_tables(
                stats["endpoint_keys"], suffix=args.expanded_suffix
            )
            for e in expansions:
                logger.info(
                    f"Expanded: {e['expanded_table']} | cols={e['column_count']} | raw_rows={e['raw_row_count']}"
                )
        else:
            logger.info("Per-endpoint expansion skipped (--skip-expand set)")
        logger.info("=" * 72)
    finally:
        etl.engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
