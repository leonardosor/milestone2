#!/usr/bin/env python3
"""
Database Connector Component for Streamlit Application

Provides database connectivity, query execution, and data retrieval
functionality for the Streamlit interface.
"""

import os
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, inspect, text


class DatabaseConnector:
    """Database connection and query utilities for Streamlit."""

    def __init__(self):
        """Initialize database connection using environment variables."""
        self.engine = None
        self._create_engine()

    def _create_engine(self):
        """Create SQLAlchemy engine from environment variables."""
        try:
            host = os.getenv("DB_HOST", "localhost")
            port = os.getenv("DB_PORT", "5432")
            database = os.getenv("DB_NAME", "milestone2")
            username = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASSWORD", "postgres")

            conn_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"

            # Handle Supabase connections
            if "supabase.co" in host:
                conn_string = f"postgresql://{username}:{password}@{host}:6543/{database}?sslmode=require"

            self.engine = create_engine(conn_string, pool_pre_ping=True)

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
        query = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
        """

        try:
            return pd.read_sql(
                query, _self.engine, params={"schema": schema, "table": table}
            )
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
            return pd.read_sql(query, self.engine)
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

            return pd.read_sql(query, self.engine)
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
