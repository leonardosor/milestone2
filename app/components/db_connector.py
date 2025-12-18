#!/usr/bin/env python3
"""
Database Connector Component for Streamlit Application

Provides database connectivity, query execution, and data retrieval
functionality for the Streamlit interface.
"""

import os
from typing import Dict, List
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


class DatabaseConnector:
    """Database connection and query utilities for Streamlit."""

    def __init__(self):
        """Initialize database connection using environment variables."""
        self.engine = None
        self._create_engine()

    @staticmethod
    def _make_arrow_compatible(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert DataFrame columns to Arrow-compatible types.

        This fixes the Streamlit serialization error with object dtypes
        by converting them to strings.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with Arrow-compatible types
        """
        if df.empty:
            return df

        df_copy = df.copy()

        # Convert object columns to strings
        for col in df_copy.columns:
            if df_copy[col].dtype == "object":
                # Try to convert to string, handling None/NaN values
                df_copy[col] = df_copy[col].astype(str)
                # Replace 'None' and 'nan' strings with empty string
                df_copy[col] = df_copy[col].replace(["None", "nan", "NaT"], "")

        return df_copy

    def _create_engine(self):
        """Create SQLAlchemy engine from environment variables or Streamlit secrets."""
        try:
            # Try to get credentials from Streamlit secrets first (for Cloud deployment)
            use_secrets = False
            try:
                if hasattr(st, "secrets") and "database" in st.secrets:
                    use_secrets = True
            except Exception:
                # Secrets file doesn't exist, use environment variables
                pass

            if use_secrets:
                host = st.secrets["database"]["DB_HOST"]
                port = st.secrets["database"].get("DB_PORT", "5432")
                database = st.secrets["database"]["DB_NAME"]
                username = st.secrets["database"]["DB_USER"]
                password = st.secrets["database"]["DB_PASSWORD"]
                schema = st.secrets["database"].get("DB_SCHEMA", "public")
            else:
                # Use environment variables (for Docker/Railway/local)
                host = os.getenv("DB_HOST", "localhost")
                port = os.getenv("DB_PORT", "5432")
                database = os.getenv("DB_NAME", "milestone2")
                username = os.getenv("DB_USER", "postgres")
                password = os.getenv("DB_PASSWORD", "postgres")
                schema = os.getenv("DB_SCHEMA", "public")

            # URL-encode the password to handle special characters
            encoded_password = quote_plus(password)

            # Build connection string
            # Cloud databases (Neon, Supabase, etc.) require SSL
            if any(
                cloud in host for cloud in ["neon.tech", "supabase.co", "aws", "azure"]
            ):
                conn_string = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}?sslmode=require"
            else:
                conn_string = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}"

            # Create engine with simple, reliable settings
            # Neon pooler doesn't support search_path in connect_args
            if "neon.tech" in host and "-pooler" in host:
                # Use Neon pooler without search_path
                self.engine = create_engine(
                    conn_string,
                    pool_pre_ping=True,
                    pool_size=5,
                    max_overflow=10,
                    pool_recycle=3600,
                )
            else:
                # For other databases, use search_path
                self.engine = create_engine(
                    conn_string,
                    pool_pre_ping=True,
                    pool_size=5,
                    max_overflow=10,
                    pool_recycle=3600,
                    connect_args={"options": f"-c search_path={schema},public"},
                )

        except Exception as e:
            st.error(f"Failed to create database engine: {e}")
            self.engine = None

    @st.cache_resource
    def get_engine(_self):
        """Return cached engine instance."""
        return _self.engine

    def test_connection(self) -> bool:
        """
        Test database connectivity.

        Returns:
            True if connection successful, False otherwise
        """
        if not self.engine:
            return False

        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            st.error(f"Connection test failed: {e}")
            return False

    @st.cache_data(ttl=60)
    def list_schemas(_self) -> List[str]:
        """
        List all user schemas in the database.

        Returns:
            List of schema names
        """
        if not _self.engine:
            return []

        query = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """

        try:
            with _self.engine.connect() as conn:
                result = conn.execute(text(query))
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            st.error(f"Error listing schemas: {e}")
            return []

    @st.cache_data(ttl=60)
    def list_tables(_self, schema: str) -> List[str]:
        """
        List all tables in a schema.

        Args:
            schema: Schema name

        Returns:
            List of table names
        """
        if not _self.engine:
            return []

        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        try:
            with _self.engine.connect() as conn:
                result = conn.execute(text(query), {"schema": schema})
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            st.error(f"Error listing tables: {e}")
            return []

    @st.cache_data(ttl=300)
    def describe_table(_self, schema: str, table: str) -> pd.DataFrame:
        """
        Get table column information.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            DataFrame with column information
        """
        query = text(
            """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
        """
        )

        try:
            with _self.engine.connect() as conn:
                result = conn.execute(query, {"schema": schema, "table": table})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return _self._make_arrow_compatible(df)
        except Exception as e:
            st.error(f"Error describing table: {e}")
            return pd.DataFrame()

    def get_table_row_count(self, schema: str, table: str) -> int:
        """
        Get the number of rows in a table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Number of rows
        """
        query = f"SELECT COUNT(*) FROM {schema}.{table}"

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return result.fetchone()[0]
        except Exception as e:
            st.error(f"Error getting row count: {e}")
            return 0

    def get_table_data(
        self, schema: str, table: str, limit: int = 100, offset: int = 0
    ) -> pd.DataFrame:
        """
        Fetch table data with pagination.

        Args:
            schema: Schema name
            table: Table name
            limit: Number of rows to fetch
            offset: Number of rows to skip

        Returns:
            DataFrame with table data
        """
        query = f"SELECT * FROM {schema}.{table} LIMIT {limit} OFFSET {offset}"

        try:
            df = pd.read_sql(query, self.engine)
            return self._make_arrow_compatible(df)
        except Exception as e:
            st.error(f"Error fetching table data: {e}")
            return pd.DataFrame()

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a custom SQL query.

        Args:
            query: SQL query string

        Returns:
            DataFrame with query results
        """
        if not self.engine:
            st.error("No database connection available")
            return pd.DataFrame()

        try:
            # Check if query is SELECT (read-only)
            query_lower = query.strip().lower()
            if not query_lower.startswith("select"):
                st.warning("Only SELECT queries are allowed in the web interface")
                return pd.DataFrame()

            df = pd.read_sql(query, self.engine)
            return self._make_arrow_compatible(df)
        except Exception as e:
            st.error(f"Query execution error: {e}")
            return pd.DataFrame()

    def get_table_info(self, schema: str, table: str) -> Dict:
        """
        Get comprehensive table information.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Dictionary with table metadata
        """
        info = {
            "schema": schema,
            "table": table,
            "row_count": self.get_table_row_count(schema, table),
            "columns": self.describe_table(schema, table),
        }

        return info


# Singleton instance
_db_connector = None


def get_db_connector() -> DatabaseConnector:
    """Get or create singleton DatabaseConnector instance."""
    global _db_connector
    if _db_connector is None:
        _db_connector = DatabaseConnector()
    return _db_connector
