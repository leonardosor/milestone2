#!/usr/bin/env python3
"""
dbt Manager Page

Manage and execute dbt transformations:
- Run models (staging, marts)
- Run tests
- View documentation
"""

import subprocess
from datetime import datetime

import streamlit as st

# Page configuration
st.set_page_config(
    page_title="dbt Manager",
    page_icon="🔧",
    layout="wide",
)

st.title("🔧 dbt Transformation Manager")
st.markdown("Execute and manage data transformation models")

# Tabs for different dbt operations
tab1, tab2, tab3, tab4 = st.tabs(["Run Models", "Run Tests", "Generate Docs", "Help"])

# Tab 1: Run Models
with tab1:
    st.subheader("Execute dbt Models")

    st.info(
        """
        dbt models transform raw data into analytics-ready tables.
        - **Staging**: Clean and standardize raw data
        - **Marts**: Business logic and aggregations
        """
    )

    # Model selection
    model_option = st.selectbox(
        "Select Models to Run",
        [
            "All Models",
            "Staging Only",
            "Marts Only",
            "Specific Model",
        ],
    )

    specific_model = None
    if model_option == "Specific Model":
        specific_model = st.text_input("Model Name", placeholder="stg_census_data")

    # Target environment
    target = st.selectbox("Target Environment", ["dev", "prod"], index=0)

    # Run button
    if st.button("▶️ Run dbt Models", type="primary", use_container_width=True):
        with st.spinner("Running dbt models..."):
            try:
                # Build command
                if model_option == "All Models":
                    cmd = [
                        "docker",
                        "exec",
                        "milestone2-etl",
                        "./entrypoint.sh",
                        "dbt-run",
                    ]
                elif model_option == "Staging Only":
                    cmd = [
                        "docker",
                        "exec",
                        "milestone2-etl",
                        "bash",
                        "-c",
                        f"cd /app/dbt_project && dbt run --select staging.* --profiles-dir . --target {target}",
                    ]
                elif model_option == "Marts Only":
                    cmd = [
                        "docker",
                        "exec",
                        "milestone2-etl",
                        "bash",
                        "-c",
                        f"cd /app/dbt_project && dbt run --select marts.* --profiles-dir . --target {target}",
                    ]
                else:  # Specific model
                    if not specific_model:
                        st.error("Please enter a model name")
                        st.stop()
                    cmd = [
                        "docker",
                        "exec",
                        "milestone2-etl",
                        "bash",
                        "-c",
                        f"cd /app/dbt_project && dbt run --select {specific_model} --profiles-dir . --target {target}",
                    ]

                # Execute
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=600
                )

                if result.returncode == 0:
                    st.success("✅ dbt models executed successfully!")
                    st.code(result.stdout, language="text")
                else:
                    st.error("❌ dbt execution failed!")
                    st.code(result.stderr, language="text")

            except subprocess.TimeoutExpired:
                st.error("⏱️ dbt execution timed out (max 10 minutes)")
            except FileNotFoundError:
                st.error("❌ Docker not found or containers not running")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

# Tab 2: Run Tests
with tab2:
    st.subheader("Run dbt Tests")

    st.info(
        """
        dbt tests validate data quality:
        - Unique constraints
        - Not null checks
        - Accepted values
        - Relationship integrity
        """
    )

    test_option = st.selectbox(
        "Select Tests to Run",
        [
            "All Tests",
            "Staging Tests Only",
            "Marts Tests Only",
        ],
    )

    # Run tests button
    if st.button("▶️ Run dbt Tests", type="primary", use_container_width=True):
        with st.spinner("Running dbt tests..."):
            try:
                # Build command
                if test_option == "All Tests":
                    cmd = [
                        "docker",
                        "exec",
                        "milestone2-etl",
                        "./entrypoint.sh",
                        "dbt-test",
                    ]
                elif test_option == "Staging Tests Only":
                    cmd = [
                        "docker",
                        "exec",
                        "milestone2-etl",
                        "bash",
                        "-c",
                        "cd /app/dbt_project && dbt test --select staging.* --profiles-dir .",
                    ]
                else:  # Marts Tests Only
                    cmd = [
                        "docker",
                        "exec",
                        "milestone2-etl",
                        "bash",
                        "-c",
                        "cd /app/dbt_project && dbt test --select marts.* --profiles-dir .",
                    ]

                # Execute
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=300
                )

                if result.returncode == 0:
                    st.success("✅ All dbt tests passed!")
                    st.code(result.stdout, language="text")
                else:
                    st.warning("⚠️ Some tests failed")
                    st.code(result.stderr, language="text")

            except subprocess.TimeoutExpired:
                st.error("⏱️ Test execution timed out")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

# Tab 3: Generate Docs
with tab3:
    st.subheader("Generate dbt Documentation")

    st.info(
        """
        dbt automatically generates documentation from your models, including:
        - Model lineage (DAG)
        - Column descriptions
        - Data tests
        - Source information
        """
    )

    if st.button("📚 Generate Documentation", type="primary", use_container_width=True):
        with st.spinner("Generating documentation..."):
            try:
                cmd = [
                    "docker",
                    "exec",
                    "milestone2-etl",
                    "./entrypoint.sh",
                    "dbt-docs",
                ]
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=120
                )

                if result.returncode == 0:
                    st.success("✅ Documentation generated successfully!")
                    st.code(result.stdout, language="text")
                    st.info(
                        """
                        📖 To view the documentation, run:
                        ```bash
                        docker exec -it milestone2-etl bash
                        cd /app/dbt_project
                        dbt docs serve --port 8080
                        ```
                        Then open http://localhost:8080 in your browser.
                        """
                    )
                else:
                    st.error("❌ Documentation generation failed")
                    st.code(result.stderr, language="text")

            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

# Tab 4: Help
with tab4:
    st.subheader("dbt Manager Help")

    st.markdown(
        """
        ## What is dbt?

        **dbt (data build tool)** transforms raw data in your warehouse into analytics-ready tables using SQL.

        ## Project Structure

        ```
        dbt_project/
        ├── models/
        │   ├── staging/        # Clean and standardize raw data
        │   │   ├── stg_census_data.sql
        │   │   └── stg_urban_data.sql
        │   └── marts/          # Business logic and aggregations
        │       ├── dim_location.sql
        │       └── fact_education.sql
        ├── dbt_project.yml     # Project configuration
        └── profiles.yml        # Database connection
        ```

        ## Model Types

        ### Staging Models (stg_*)
        - Clean raw data from ETL
        - Standardize column names
        - Handle null values
        - Apply basic data types

        ### Mart Models (dim_*, fact_*)
        - Implement business logic
        - Create aggregations
        - Join multiple sources
        - Build dimensional models

        ## Common Workflows

        ### 1. After ETL
        ```
        1. Run ETL pipelines
        2. Run staging models
        3. Run tests on staging
        4. Run mart models
        5. Run final tests
        ```

        ### 2. Development
        ```
        1. Modify SQL model
        2. Run specific model
        3. Test the changes
        4. Generate documentation
        ```

        ### 3. Production
        ```
        1. Run all models
        2. Run all tests
        3. Verify data quality
        4. Generate docs
        ```

        ## dbt Commands Reference

        | Command | Description |
        |---------|-------------|
        | `dbt run` | Execute all models |
        | `dbt test` | Run all tests |
        | `dbt run --select staging.*` | Run staging models only |
        | `dbt test --select marts.*` | Test marts only |
        | `dbt docs generate` | Create documentation |

        ## Tips

        - Always run staging before marts
        - Test after each transformation
        - Use incremental models for large tables
        - Document your models with descriptions

        ## Troubleshooting

        **Models fail to run:**
        - Check database connection
        - Verify source tables exist
        - Review SQL syntax

        **Tests fail:**
        - Review test definitions
        - Check data quality
        - Examine test output for details
        """
    )

# Footer
st.markdown("---")
st.caption(
    f"dbt Manager • Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)
