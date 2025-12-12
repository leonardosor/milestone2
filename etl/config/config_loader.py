#!/usr/bin/env python3
"""
Centralized Configuration Loader with Environment Variable Override Support

This module provides a unified configuration interface that:
1. Loads configuration from JSON files
2. Overrides with environment variables
3. Provides database connection strings
4. Supports both local PostgreSQL and Supabase
"""

import json
import os
from pathlib import Path
from typing import Dict, Optional


class ConfigLoader:
    """Centralized configuration loader with env var override support."""

    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize ConfigLoader.

        Args:
            config_file: Path to config JSON file. If None, uses APP_CONFIG env var
                        or searches default locations.
        """
        self.config_file = config_file or os.getenv("APP_CONFIG", "config.json")
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """Load config with fallbacks and env var overrides."""
        # 1. Load from file
        config = self._load_json_file()

        # 2. Override with environment variables
        config = self._apply_env_overrides(config)

        return config

    def _load_json_file(self) -> Dict:
        """Load config.json with search paths."""
        search_paths = [
            Path(self.config_file),
            Path.cwd() / self.config_file,
            Path(__file__).parent / self.config_file,
            Path(__file__).parent.parent / self.config_file,
            Path(__file__).parent.parent.parent / self.config_file,
            Path("/app/config/config.json"),
            Path("/app/config.json"),
        ]

        for path in search_paths:
            if path.exists() and path.is_file():
                try:
                    with open(path, "r") as f:
                        loaded_config = json.load(f)
                    print(f"[ConfigLoader] Loaded config from: {path}")
                    return loaded_config
                except json.JSONDecodeError as e:
                    print(f"[ConfigLoader] Invalid JSON in {path}: {e}")
                    continue

        # Return defaults if no file found
        print("[ConfigLoader] No config file found, using defaults")
        return self._get_defaults()

    def _apply_env_overrides(self, config: Dict) -> Dict:
        """Override config with environment variables."""
        # Database type
        db_type = os.getenv("DATABASE_TYPE", config.get("database_type", "local"))
        config["database_type"] = db_type

        # Database config from env vars
        db_key = "local_database" if db_type == "local" else "env_database"

        # Ensure the db_key exists in config
        if db_key not in config:
            config[db_key] = {}

        config[db_key] = {
            "host": os.getenv(
                "DB_HOST", config.get(db_key, {}).get("host", "localhost")
            ),
            "port": int(os.getenv("DB_PORT", config.get(db_key, {}).get("port", 5432))),
            "database": os.getenv(
                "DB_NAME", config.get(db_key, {}).get("database", "milestone2")
            ),
            "username": os.getenv(
                "DB_USER", config.get(db_key, {}).get("username", "postgres")
            ),
            "password": os.getenv(
                "DB_PASSWORD", config.get(db_key, {}).get("password", "postgres")
            ),
        }

        # Schema
        config["schema"] = os.getenv("DB_SCHEMA", config.get("schema", "test"))

        # ETL config overrides
        if "etl" not in config:
            config["etl"] = {}

        if os.getenv("ETL_CENSUS_BEGIN_YEAR"):
            census_begin = int(os.getenv("ETL_CENSUS_BEGIN_YEAR"))
            census_end = int(os.getenv("ETL_CENSUS_END_YEAR", census_begin))
            config["etl"]["census_years"] = [census_begin, census_end]

        if os.getenv("ETL_URBAN_BEGIN_YEAR"):
            urban_begin = int(os.getenv("ETL_URBAN_BEGIN_YEAR"))
            urban_end = int(os.getenv("ETL_URBAN_END_YEAR", urban_begin))
            config["etl"]["urban_years"] = [urban_begin, urban_end]

        return config

    def _get_defaults(self) -> Dict:
        """Default configuration."""
        return {
            "database_type": "local",
            "local_database": {
                "host": "localhost",
                "port": 5432,
                "database": "milestone2",
                "username": "postgres",
                "password": "postgres",
            },
            "schema": "test",
            "etl": {
                "census_years": [2014, 2024],
                "urban_years": [2014, 2024],
                "batch_size": 1000,
            },
            "async": {
                "max_concurrent_requests": 10,
                "db_batch_size": 1000,
                "connection_pool_size": 10,
                "max_overflow": 20,
            },
            "census": {"rate_limit_delay": 1},
            "urban": {"base_url": "https://educationdata.urban.org"},
        }

    def get_db_connection_string(self) -> str:
        """
        Get SQLAlchemy connection string for the configured database.

        Returns:
            PostgreSQL connection string

        Examples:
            postgresql://user:pass@localhost:5432/dbname
            postgresql://user:pass@project.supabase.co:5432/postgres
        """
        db_type = self.config["database_type"]
        db = self.config["local_database" if db_type == "local" else "env_database"]

        # Supabase-specific handling
        if "supabase.co" in db["host"]:
            # Supabase recommends port 6543 for connection pooling in transaction mode
            # Use sslmode=require for Supabase
            return (
                f"postgresql://{db['username']}:{db['password']}@"
                f"{db['host']}:6543/{db['database']}?sslmode=require"
            )

        # Standard PostgreSQL
        return (
            f"postgresql://{db['username']}:{db['password']}@"
            f"{db['host']}:{db['port']}/{db['database']}"
        )

    def get_psycopg2_connection_params(self) -> Dict:
        """
        Get psycopg2 connection parameters.

        Returns:
            Dict with host, port, dbname, user, password
        """
        db_type = self.config["database_type"]
        db = self.config["local_database" if db_type == "local" else "env_database"]

        params = {
            "host": db["host"],
            "port": db["port"],
            "dbname": db["database"],
            "user": db["username"],
            "password": db["password"],
        }

        # Add SSL mode for Supabase
        if "supabase.co" in db["host"]:
            params["sslmode"] = "require"

        return params

    def get(self, key: str, default=None):
        """Get a config value by key with optional default."""
        return self.config.get(key, default)

    def __getitem__(self, key: str):
        """Allow dict-like access to config."""
        return self.config[key]

    def __contains__(self, key: str) -> bool:
        """Allow 'in' operator for config keys."""
        return key in self.config


# Convenience function for backward compatibility
def load_config(config_file: str = "config.json") -> Dict:
    """
    Load configuration (backward compatible with existing code).

    Args:
        config_file: Path to config file

    Returns:
        Configuration dictionary
    """
    loader = ConfigLoader(config_file)
    return loader.config


if __name__ == "__main__":
    # Test configuration loading
    loader = ConfigLoader()
    print("\n=== Configuration Loaded ===")
    print(f"Database Type: {loader.config['database_type']}")
    print(f"Connection String: {loader.get_db_connection_string()}")
    print(f"Schema: {loader.config['schema']}")
    print(f"ETL Config: {loader.config.get('etl', {})}")
    print("\n=== psycopg2 Params ===")
    print(loader.get_psycopg2_connection_params())
