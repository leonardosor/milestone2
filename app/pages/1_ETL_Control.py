#!/usr/bin/env python3
"""
ETL Pipeline Control Page

Provides interface to trigger and monitor ETL pipelines for:
- Census Data
- Urban Institute Data
- Location Data (Geocoding)
"""

import asyncio
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

# Page configuration
st.set_page_config(
    page_title="ETL Control",
    page_icon="🔄",
    layout="wide",
)

st.title("🔄 ETL Pipeline Control")
st.markdown("Configure and execute data extraction pipelines")

# Add ETL modules to path
sys.path.append(str(Path(__file__).parent.parent / "etl_modules"))


# Detect execution environment
@st.cache_data(ttl=30)
def get_execution_mode():
    """
    Determine execution mode:
    - 'docker': Running inside Docker with access to docker socket
    - 'container': Running inside Docker container (direct execution)
    - 'local': Running on host machine
    """
    # Check if running inside a container
    if os.path.exists("/.dockerenv") or os.path.exists("/app/etl_modules"):
        # We're inside a container - use direct execution
        return "container"

    # Check if docker command is available for docker exec
    try:
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                "name=milestone2-etl",
                "--format",
                "{{.Names}}",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            stderr=subprocess.DEVNULL,
        )
        if "milestone2-etl" in result.stdout:
            return "docker"
    except:
        pass

    return "local"


EXECUTION_MODE = get_execution_mode()

# Display mode indicator
if EXECUTION_MODE == "container":
    st.success(
        "✅ **CONTAINER MODE**: Running inside Docker - ETL will execute directly"
    )
elif EXECUTION_MODE == "docker":
    st.info("✅ **DOCKER MODE**: Using docker exec for ETL services")
else:
    st.warning("⚠️ **LOCAL MODE**: Limited functionality - Docker not available")

# ETL Pipeline Selection
st.sidebar.header("Pipeline Configuration")

pipeline_type = st.sidebar.selectbox(
    "Select Pipeline",
    [
        "Complete Pipeline (All ETLs)",
        "Census Data Only",
        "Urban Institute Data Only",
        "Location Data Only",
    ],
)

# Year Configuration
st.sidebar.subheader("Year Range")

col1, col2 = st.sidebar.columns(2)
with col1:
    census_begin = st.number_input(
        "Census Begin", value=2014, min_value=2010, max_value=2024, step=1
    )
with col2:
    census_end = st.number_input(
        "Census End", value=2024, min_value=2010, max_value=2024, step=1
    )

col3, col4 = st.sidebar.columns(2)
with col3:
    urban_begin = st.number_input(
        "Urban Begin", value=2014, min_value=2010, max_value=2024, step=1
    )
with col4:
    urban_end = st.number_input(
        "Urban End", value=2024, min_value=2010, max_value=2024, step=1
    )

# Advanced Options
st.sidebar.subheader("Advanced Options")
skip_census = st.sidebar.checkbox("Skip Census ETL", value=False)
skip_urban = st.sidebar.checkbox("Skip Urban ETL", value=False)
skip_location = st.sidebar.checkbox("Skip Location ETL", value=False)

# Main content area
tab1, tab2, tab3 = st.tabs(["Pipeline Execution", "Status & Logs", "Help"])

with tab1:
    st.subheader("Execute ETL Pipeline")

    st.info(
        """
        **Important**: ETL pipelines can take significant time to complete:
        - Census ETL: 5-30 minutes depending on year range
        - Urban ETL: 10-60 minutes depending on endpoints and years
        - Location ETL: 15-45 minutes (includes shapefile download)
        """
    )

    # Pipeline summary
    st.markdown("### Pipeline Summary")
    col_a, col_b = st.columns(2)

    with col_a:
        st.metric("Census Years", f"{census_begin} - {census_end}")
        st.metric("Skip Census", "Yes" if skip_census else "No")

    with col_b:
        st.metric("Urban Years", f"{urban_begin} - {urban_end}")
        st.metric("Skip Urban", "Yes" if skip_urban else "No")

    # Execute button
    st.markdown("---")

    # Disable button in local mode only
    button_disabled = EXECUTION_MODE == "local"
    if button_disabled:
        st.warning(
            """
        ⚠️ **ETL execution requires Docker**

        **To run ETL pipelines:**

        1. **Via Docker** (Recommended):
           ```bash
           docker-compose -f docker-compose.prod.yml up --build
           ```
           Then access this page at http://localhost:8501

        2. **Via Command Line** (Alternative):
           ```bash
           python etl/src/main.py --census-only --census-begin-year 2020 --census-end-year 2023
           ```
        """
        )

    if st.button(
        "▶️ Run Pipeline",
        type="primary",
        use_container_width=True,
        disabled=button_disabled,
    ):
        with st.spinner(f"Running {pipeline_type}..."):
            try:
                if EXECUTION_MODE == "docker":
                    # Build command based on pipeline type (Docker mode)
                    if pipeline_type == "Complete Pipeline (All ETLs)":
                        cmd = [
                            "docker",
                            "exec",
                            "milestone2-etl",
                            "python",
                            "/app/src/main.py",
                            "--census-begin-year",
                            str(census_begin),
                            "--census-end-year",
                            str(census_end),
                            "--urban-begin-year",
                            str(urban_begin),
                            "--urban-end-year",
                            str(urban_end),
                        ]
                        if skip_census:
                            cmd.append("--skip-census")
                        if skip_urban:
                            cmd.append("--skip-urban")
                        if skip_location:
                            cmd.append("--skip-location")

                    elif pipeline_type == "Census Data Only":
                        cmd = [
                            "docker",
                            "exec",
                            "milestone2-etl",
                            "python",
                            "/app/src/main.py",
                            "--census-only",
                            "--census-begin-year",
                            str(census_begin),
                            "--census-end-year",
                            str(census_end),
                        ]

                    elif pipeline_type == "Urban Institute Data Only":
                        cmd = [
                            "docker",
                            "exec",
                            "milestone2-etl",
                            "python",
                            "/app/src/main.py",
                            "--urban-only",
                            "--urban-begin-year",
                            str(urban_begin),
                            "--urban-end-year",
                            str(urban_end),
                        ]

                    else:  # Location Data Only
                        cmd = [
                            "docker",
                            "exec",
                            "milestone2-etl",
                            "python",
                            "/app/src/main.py",
                            "--location-only",
                        ]

                    # Execute command
                    result = subprocess.run(
                        cmd, capture_output=True, text=True, timeout=3600
                    )

                    if result.returncode == 0:
                        st.success("✅ ETL pipeline completed successfully!")
                        st.code(result.stdout, language="text")
                    else:
                        st.error("❌ ETL pipeline failed!")
                        st.code(result.stderr, language="text")

                elif EXECUTION_MODE == "container":
                    # Container mode - execute directly
                    st.info("🔄 Executing ETL in container mode...")

                    try:
                        # Import ETL controller
                        from main import OrchestatedETLController

                        # Change to /app directory where config.json should be
                        original_dir = os.getcwd()
                        if os.path.exists("/app"):
                            os.chdir("/app")

                        etl_controller = OrchestatedETLController("config.json")

                        # Execute based on pipeline type
                        if pipeline_type == "Census Data Only":
                            etl_controller.run_census_etl(census_begin, census_end)
                            st.success("✅ Census ETL completed successfully!")

                        elif pipeline_type == "Urban Institute Data Only":
                            asyncio.run(
                                etl_controller.run_urban_etl(urban_begin, urban_end)
                            )
                            st.success("✅ Urban ETL completed successfully!")

                        elif pipeline_type == "Location Data Only":
                            etl_controller.run_location_etl()
                            st.success("✅ Location ETL completed successfully!")

                        else:  # Complete Pipeline
                            asyncio.run(
                                etl_controller.run_complete_pipeline(
                                    census_begin_year=census_begin,
                                    census_end_year=census_end,
                                    urban_begin_year=urban_begin,
                                    urban_end_year=urban_end,
                                    skip_census=skip_census,
                                    skip_urban=skip_urban,
                                    skip_location=skip_location,
                                )
                            )
                            st.success("✅ Complete pipeline executed successfully!")

                        # Restore directory
                        os.chdir(original_dir)

                    except Exception as e:
                        st.error(f"❌ Container ETL execution failed: {str(e)}")
                        import traceback

                        st.code(traceback.format_exc(), language="text")

            except subprocess.TimeoutExpired:
                st.error("⏱️ Pipeline timed out (max 1 hour)")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                import traceback

                with st.expander("Show detailed error"):
                    st.code(traceback.format_exc(), language="text")

with tab2:
    st.subheader("Pipeline Status & Logs")

    if st.button("🔄 Refresh Logs"):
        try:
            if EXECUTION_MODE == "docker":
                log_cmd = [
                    "docker",
                    "exec",
                    "milestone2-etl",
                    "tail",
                    "-n",
                    "100",
                    "/app/logs/main_etl.log",
                ]
                log_result = subprocess.run(
                    log_cmd, capture_output=True, text=True, timeout=10
                )

                if log_result.returncode == 0:
                    st.text_area(
                        "Recent Logs (Last 100 lines)", log_result.stdout, height=400
                    )
                else:
                    st.warning(
                        "Could not retrieve logs. The log file may not exist yet."
                    )

            elif EXECUTION_MODE == "container" or EXECUTION_MODE == "local":
                # Container/Local mode - read logs directly
                possible_log_paths = [
                    Path("/app/logs/main_etl.log"),  # Container path
                    Path(__file__).parent.parent.parent
                    / "logs"
                    / "main_etl.log",  # Local path
                ]

                log_found = False
                for log_file in possible_log_paths:
                    if log_file.exists():
                        with open(log_file, "r") as f:
                            lines = f.readlines()
                            last_100 = (
                                "".join(lines[-100:]) if lines else "Log file is empty"
                            )
                            st.text_area(
                                "Recent Logs (Last 100 lines)", last_100, height=400
                            )
                        log_found = True
                        break

                if not log_found:
                    st.info("No log files found yet. Run a pipeline to generate logs.")

        except Exception as e:
            st.warning(f"Could not retrieve logs: {str(e)}")

    st.markdown("---")

    # Container status
    st.markdown("### System Status")
    if st.button("Check System Status"):
        try:
            if EXECUTION_MODE == "docker":
                status_cmd = [
                    "docker",
                    "ps",
                    "--filter",
                    "name=milestone2",
                    "--format",
                    "table {{.Names}}\t{{.Status}}",
                ]
                status_result = subprocess.run(
                    status_cmd, capture_output=True, text=True
                )
                st.code(status_result.stdout, language="text")
            elif EXECUTION_MODE == "container":
                st.info(
                    f"✅ Running inside container\n\nExecution mode: {EXECUTION_MODE}\nWorking directory: {os.getcwd()}"
                )
            else:
                st.info("Running in local mode - no Docker containers to check")
        except Exception as e:
            st.error(f"Could not check status: {str(e)}")

with tab3:
    st.subheader("ETL Pipeline Help")

    st.markdown(
        """
        ## Data Sources

        ### 1. Census Data ETL
        - **Source**: US Census Bureau API
        - **Data**: American Community Survey (ACS) 5-year estimates
        - **Includes**: Demographics, household income, age distributions
        - **Geographic Level**: ZIP Code Tabulation Areas (ZCTAs)

        ### 2. Urban Institute Data ETL
        - **Source**: Urban Institute Education Data API
        - **Data**: Education statistics and school information
        - **Endpoints**: Schools directory, enrollment data
        - **No API key required**

        ### 3. Location Data ETL
        - **Source**: US Census Bureau TIGER/Line shapefiles
        - **Process**: Offline geocoding with GeoPandas
        - **Output**: County, state, and ZIP code boundaries
        - **Note**: First run downloads ~500MB of shapefiles

        ## Pipeline Options

        ### Complete Pipeline
        Runs all three ETL processes in sequence:
        1. Census Data Collection
        2. Urban Institute Data Collection
        3. Location Data Processing

        ### Individual Pipelines
        Run specific ETL processes independently for faster iterations or troubleshooting.

        ## Tips

        - **First Time**: Run Location ETL separately first (downloads shapefiles)
        - **Testing**: Use small year ranges (e.g., 2020-2021)
        - **Production**: Use full year ranges for complete datasets
        - **Monitoring**: Check logs regularly for errors or warnings

        ## Common Issues

        1. **Timeout**: Increase timeout or run pipelines individually
        2. **API Rate Limits**: Census API may rate limit; ETL includes delays
        3. **Disk Space**: Ensure sufficient space for shapefiles and data
        4. **Memory**: Large year ranges may require more RAM
        """
    )

# Footer
st.markdown("---")
st.caption(
    f"ETL Control Panel • Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)
