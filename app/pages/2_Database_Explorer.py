#!/usr/bin/env python3
"""
Database Explorer Page

Interactive database browser with:
- Schema and table navigation
- Data preview and filtering
- Custom SQL queries
- Export functionality
"""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Add components to path
sys.path.append(str(Path(__file__).parent.parent))
from components.db_connector import get_db_connector

# Page configuration
st.set_page_config(
    page_title="Database Explorer",
    page_icon="🗄️",
    layout="wide",
)

st.title("🗄️ Database Explorer")
st.markdown("Browse schemas, tables, and run custom queries")

# Initialize database connector
db = get_db_connector()

# Test connection
if not db.test_connection():
    st.error("❌ Cannot connect to database. Check your configuration.")
    st.stop()

# Sidebar - Schema and Table Selection
st.sidebar.header("Database Navigation")

# Schema selection
schemas = db.list_schemas()
if not schemas:
    st.sidebar.error("No schemas found")
    st.stop()

selected_schema = st.sidebar.selectbox("Select Schema", schemas)

# Table selection
if selected_schema:
    tables = db.list_tables(selected_schema)
    if tables:
        selected_table = st.sidebar.selectbox("Select Table", tables)
    else:
        st.sidebar.warning(f"No tables in schema '{selected_schema}'")
        selected_table = None
else:
    selected_table = None

# Main content tabs
tab1, tab2, tab3 = st.tabs(["Table Browser", "Custom Query", "Schema Info"])

# Tab 1: Table Browser
with tab1:
    if selected_table:
        st.subheader(f"Table: {selected_schema}.{selected_table}")

        # Get table info
        table_info = db.get_table_info(selected_schema, selected_table)
        row_count = table_info["row_count"]
        columns_df = table_info["columns"]

        # Display table metadata
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Rows", f"{row_count:,}")
        with col2:
            st.metric("Columns", len(columns_df))

        # Show table structure
        with st.expander("📋 Table Structure", expanded=False):
            st.dataframe(columns_df, use_container_width=True)

        # Data preview options
        st.markdown("### Data Preview")

        col_a, col_b = st.columns([3, 1])
        with col_a:
            limit = st.slider("Number of rows", 10, 1000, 100, step=10)
        with col_b:
            offset = st.number_input("Offset", min_value=0, value=0, step=100)

        # Fetch and display data
        try:
            df = db.get_table_data(
                selected_schema, selected_table, limit=limit, offset=offset
            )

            if not df.empty:
                st.dataframe(df, use_container_width=True)

                # Download button
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"{selected_schema}_{selected_table}.csv",
                    mime="text/csv",
                )

                # Data summary
                with st.expander("📊 Data Summary"):
                    st.write("**Data Types:**")
                    st.write(df.dtypes)
                    st.write("**Missing Values:**")
                    st.write(df.isnull().sum())
                    st.write("**Basic Statistics:**")
                    st.write(df.describe())
            else:
                st.info("Table is empty or no data available")

        except Exception as e:
            st.error(f"Error fetching data: {e}")

    else:
        st.info("👈 Select a schema and table from the sidebar to begin exploring")

# Tab 2: Custom Query
with tab2:
    st.subheader("Custom SQL Query")

    st.info(
        """
        **Note**: Only SELECT queries are allowed for safety.
        Write operations should be done through ETL pipelines.
        """
    )

    # Query input
    default_query = (
        f"SELECT * FROM {selected_schema}.{selected_table} LIMIT 100"
        if selected_table
        else "SELECT 1"
    )

    query = st.text_area("SQL Query", default_query, height=150)

    col1, col2 = st.columns([1, 5])
    with col1:
        execute_button = st.button("▶️ Execute", type="primary")

    if execute_button:
        with st.spinner("Executing query..."):
            try:
                result_df = db.execute_query(query)

                if not result_df.empty:
                    st.success(
                        f"✅ Query executed successfully! ({len(result_df)} rows returned)"
                    )

                    # Display results
                    st.dataframe(result_df, use_container_width=True)

                    # Download button
                    csv = result_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Results as CSV",
                        data=csv,
                        file_name="query_results.csv",
                        mime="text/csv",
                    )

                    # Show query execution stats
                    with st.expander("Query Details"):
                        st.write(f"**Rows returned**: {len(result_df)}")
                        st.write(f"**Columns**: {len(result_df.columns)}")
                        st.write(f"**Column names**: {', '.join(result_df.columns)}")
                else:
                    st.warning("Query executed but returned no results")

            except Exception as e:
                st.error(f"Query error: {e}")

    # Query templates
    with st.expander("📝 Query Templates"):
        st.markdown(
            """
            **Common Queries:**

            ```sql
            -- Count rows in a table
            SELECT COUNT(*) FROM schema_name.table_name;

            -- View recent records
            SELECT * FROM schema_name.table_name ORDER BY id DESC LIMIT 100;

            -- Check for duplicates
            SELECT column_name, COUNT(*)
            FROM schema_name.table_name
            GROUP BY column_name
            HAVING COUNT(*) > 1;

            -- Join tables
            SELECT a.*, b.column_name
            FROM schema.table_a a
            JOIN schema.table_b b ON a.id = b.foreign_id;
            ```
            """
        )

# Tab 3: Schema Info
with tab3:
    st.subheader("Schema Information")

    for schema in schemas:
        with st.expander(f"📁 Schema: {schema}"):
            schema_tables = db.list_tables(schema)

            if schema_tables:
                st.write(f"**Tables in {schema}**: {len(schema_tables)}")

                # Create a summary DataFrame
                table_summary = []
                for table in schema_tables:
                    try:
                        row_count = db.get_table_row_count(schema, table)
                        columns = db.describe_table(schema, table)
                        table_summary.append(
                            {
                                "Table": table,
                                "Rows": f"{row_count:,}",
                                "Columns": len(columns),
                            }
                        )
                    except Exception:
                        table_summary.append(
                            {"Table": table, "Rows": "Error", "Columns": "Error"}
                        )

                summary_df = pd.DataFrame(table_summary)
                st.dataframe(summary_df, use_container_width=True)
            else:
                st.write("No tables in this schema")

# Footer
st.markdown("---")
st.caption(
    "Database Explorer • Connected to: " + (st.session_state.get("db_host", "Unknown"))
)
