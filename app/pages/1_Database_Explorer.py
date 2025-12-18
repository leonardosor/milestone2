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

from components.db_connector import get_db_connector  # noqa: E402

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
tab1, tab2, tab3, tab4 = st.tabs(
    ["Table Browser", "Custom Query", "Schema Info", "Visualizations"]
)

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

# Tab 4: Visualizations
with tab4:
    st.subheader("📊 Data Visualizations")

    # Focus on census_data table for visualizations
    census_schema = "test"
    census_table = "census_data"

    if db.test_connection():
        try:
            # Check if census_data table exists
            tables = db.list_tables(census_schema)
            if census_table in tables:
                st.info("🎯 Visualizing census_data table")

                # Get sample data for visualizations
                df = db.get_table_data(census_schema, census_table, limit=10000)

                if not df.empty:
                    # Visualization options
                    viz_type = st.selectbox(
                        "Select Visualization Type",
                        [
                            "Population Distribution",
                            "Income Analysis",
                            "Demographic Breakdown",
                            "Geographic Analysis",
                        ],
                    )

                    if viz_type == "Population Distribution":
                        st.markdown("### 👥 Population Distribution by ZIP Code")

                        # Filter out rows with zero population
                        pop_df = df[df["total_pop"] > 0].copy()

                        if not pop_df.empty:
                            # Create histogram of population
                            import plotly.express as px

                            fig = px.histogram(
                                pop_df,
                                x="total_pop",
                                nbins=50,
                                title="Distribution of Total Population by ZIP Code",
                                labels={"total_pop": "Total Population"},
                            )
                            fig.update_layout(
                                xaxis_title="Total Population",
                                yaxis_title="Number of ZIP Codes",
                                showlegend=False,
                            )
                            st.plotly_chart(fig, use_container_width=True)

                            # Summary statistics
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total ZIP Codes", len(pop_df))
                            with col2:
                                st.metric(
                                    "Avg Population",
                                    f"{pop_df['total_pop'].mean():,.0f}",
                                )
                            with col3:
                                st.metric(
                                    "Median Population",
                                    f"{pop_df['total_pop'].median():,.0f}",
                                )
                            with col4:
                                st.metric(
                                    "Max Population",
                                    f"{pop_df['total_pop'].max():,.0f}",
                                )

                    elif viz_type == "Income Analysis":
                        st.markdown("### 💰 Income Analysis")

                        # Calculate income percentages
                        income_df = df.copy()
                        income_df["income_150k_200k_pct"] = (
                            income_df["hhi_150k_200k"] / income_df["total_pop"] * 100
                        ).fillna(0)
                        income_df["income_220k_plus_pct"] = (
                            income_df["hhi_220k_plus"] / income_df["total_pop"] * 100
                        ).fillna(0)

                        # Filter for meaningful data
                        income_df = income_df[income_df["total_pop"] > 100]

                        if not income_df.empty:
                            # Scatter plot of income vs population
                            fig = px.scatter(
                                income_df,
                                x="total_pop",
                                y="income_150k_200k_pct",
                                size="income_220k_plus_pct",
                                title="Income Distribution vs Population Size",
                                labels={
                                    "total_pop": "Total Population",
                                    "income_150k_200k_pct": "% Households $150K-$200K",
                                    "income_220k_plus_pct": "% Households $220K+",
                                },
                                hover_data=["zip_code"],
                            )
                            st.plotly_chart(fig, use_container_width=True)

                            # Bar chart of top income areas
                            top_income = income_df.nlargest(10, "income_220k_plus_pct")
                            fig2 = px.bar(
                                top_income,
                                x="zip_code",
                                y="income_220k_plus_pct",
                                title="Top 10 ZIP Codes by % Households Earning $220K+",
                                labels={
                                    "income_220k_plus_pct": "% Households $220K+",
                                    "zip_code": "ZIP Code",
                                },
                            )
                            st.plotly_chart(fig2, use_container_width=True)

                    elif viz_type == "Demographic Breakdown":
                        st.markdown("### 👨‍👩‍👧‍👦 Demographic Analysis (Ages 10-14)")

                        # Calculate demographic percentages
                        demo_df = df.copy()
                        demo_df["total_10_14"] = (
                            demo_df["males_10_14"] + demo_df["females_10_14"]
                        )
                        demo_df["male_pct"] = (
                            demo_df["males_10_14"] / demo_df["total_10_14"] * 100
                        ).fillna(0)
                        demo_df["female_pct"] = (
                            demo_df["females_10_14"] / demo_df["total_10_14"] * 100
                        ).fillna(0)

                        # Filter for areas with children
                        demo_df = demo_df[demo_df["total_10_14"] > 0]

                        if not demo_df.empty:
                            # Gender distribution
                            fig = px.histogram(
                                demo_df,
                                x="male_pct",
                                nbins=20,
                                title="Gender Distribution of Children Ages 10-14",
                                labels={"male_pct": "% Male"},
                            )
                            fig.update_layout(
                                xaxis_title="% Male Children (Ages 10-14)",
                                yaxis_title="Number of ZIP Codes",
                            )
                            st.plotly_chart(fig, use_container_width=True)

                            # Racial/ethnic breakdown (if available)
                            race_cols = [
                                "white_males_10_14",
                                "black_males_10_14",
                                "hispanic_males_10_14",
                                "white_females_10_14",
                                "black_females_10_14",
                                "hispanic_females_10_14",
                            ]

                            if all(col in df.columns for col in race_cols):
                                # Aggregate racial data
                                race_totals = df[race_cols].sum()
                                race_data = pd.DataFrame(
                                    {
                                        "Race": [
                                            "White Males",
                                            "Black Males",
                                            "Hispanic Males",
                                            "White Females",
                                            "Black Females",
                                            "Hispanic Females",
                                        ],
                                        "Count": race_totals.values,
                                    }
                                )

                                fig2 = px.pie(
                                    race_data,
                                    values="Count",
                                    names="Race",
                                    title="Racial/Ethnic Breakdown of Children Ages 10-14",
                                )
                                st.plotly_chart(fig2, use_container_width=True)

                    elif viz_type == "Geographic Analysis":
                        st.markdown("### 🗺️ Geographic Analysis")

                        # ZIP code analysis
                        zip_df = df[df["zip_code"].notna()].copy()
                        zip_df["zip_prefix"] = zip_df["zip_code"].str[:3]

                        # Group by ZIP prefix
                        geo_summary = (
                            zip_df.groupby("zip_prefix")
                            .agg(
                                {
                                    "total_pop": "sum",
                                    "hhi_220k_plus": "sum",
                                    "zip_code": "count",
                                }
                            )
                            .reset_index()
                        )

                        geo_summary.columns = [
                            "ZIP_Prefix",
                            "Total_Population",
                            "High_Income_Households",
                            "ZIP_Code_Count",
                        ]

                        if not geo_summary.empty:
                            # Population by region
                            fig = px.bar(
                                geo_summary,
                                x="ZIP_Prefix",
                                y="Total_Population",
                                title="Total Population by ZIP Code Prefix",
                                labels={
                                    "ZIP_Prefix": "ZIP Code Prefix",
                                    "Total_Population": "Total Population",
                                },
                            )
                            st.plotly_chart(fig, use_container_width=True)

                            # High income households by region
                            fig2 = px.bar(
                                geo_summary,
                                x="ZIP_Prefix",
                                y="High_Income_Households",
                                title="High Income Households ($220K+) by ZIP Code Prefix",
                                labels={
                                    "ZIP_Prefix": "ZIP Code Prefix",
                                    "High_Income_Households": "Households $220K+",
                                },
                            )
                            st.plotly_chart(fig2, use_container_width=True)

                else:
                    st.warning("No data available in census_data table")
            else:
                st.warning(
                    f"Table '{census_table}' not found in schema '{census_schema}'"
                )
        except Exception as e:
            st.error(f"Error creating visualizations: {e}")
    else:
        st.error("Database connection not available")

# Footer
st.markdown("---")
st.caption(
    "Database Explorer • Connected to: " + (st.session_state.get("db_host", "Unknown"))
)
