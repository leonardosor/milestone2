#!/usr/bin/env python3
"""
ETL & Analytics Platform
Main Streamlit Application Entry Point
"""

import sys
from pathlib import Path

import streamlit as st

# Add ETL modules to path
sys.path.append(str(Path(__file__).parent / "etl_modules"))

# Page configuration
st.set_page_config(
    page_title="Demographic and Academic Performance",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling
st.markdown(
    """
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .feature-box {
        padding: 1.5rem;
        border-radius: 0.5rem;
        background-color: #f0f2f6;
        margin-bottom: 1rem;
    }
    .status-good {
        color: #28a745;
        font-weight: bold;
    }
    .status-bad {
        color: #dc3545;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Title
st.markdown(
    '<div class="main-header">📊 Census & Education Data Platform</div>',
    unsafe_allow_html=True,
)
st.markdown("---")

# Sidebar navigation
st.sidebar.title("🧭 Navigation")
st.sidebar.info(
    """
    **This application provides:**
    - Interactive database exploration
    - ML model insights
    """
)

st.sidebar.markdown("---")
st.sidebar.markdown("### Quick Links")
st.sidebar.markdown("- 🗄️ [Database Explorer](/1_Database_Explorer)")
st.sidebar.markdown("- 🤖 [ML Dashboard](/2_ML_Dashboard)")
st.sidebar.markdown("- 🗺️ [Interactive Analytics](/3_Interactive_Analytics)")


# Debug: Check secrets configuration
with st.expander("🔍 Debug: Secrets Configuration", expanded=False):
    import os

    st.write("**Secrets Status:**")

    secrets_exist = False
    database_in_secrets = False

    try:
        if hasattr(st, "secrets") and "database" in st.secrets:
            secrets_exist = True
            database_in_secrets = True
            st.success("✓ st.secrets exists")
            st.success("✓ 'database' section found in secrets")
            st.write("**Database config keys:**", list(st.secrets["database"].keys()))
            st.write("**DB_HOST:**", st.secrets["database"].get("DB_HOST", "NOT FOUND"))
            st.write("**DB_PORT:**", st.secrets["database"].get("DB_PORT", "NOT FOUND"))
    except Exception:
        st.info("ℹ️ No secrets file found (this is normal for Railway/Docker)")

    st.write("---")
    st.write("**Current Database Connection:**")

    # Show what values are actually being used
    if database_in_secrets:
        try:
            st.write("Using Streamlit secrets:")
            st.write("- Host:", st.secrets["database"]["DB_HOST"])
            st.write("- Port:", st.secrets["database"].get("DB_PORT", "5432"))
        except Exception:
            pass
    else:
        st.write("Using environment variables:")
        st.write("- Host:", os.getenv("DB_HOST", "localhost"))
        st.write("- Port:", os.getenv("DB_PORT", "5432"))
        st.write("- Database:", os.getenv("DB_NAME", "milestone2"))
        st.write("- User:", os.getenv("DB_USER", "postgres"))
        st.write("- Schema:", os.getenv("DB_SCHEMA", "public"))

    # Clear cache button
    if st.button("🔄 Clear Cache & Reconnect"):
        st.cache_resource.clear()
        st.cache_data.clear()
        st.success("Cache cleared! Page will reload...")
        st.rerun()

# Main page content - Feature overview
st.subheader("Welcome to the Data Platform")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🗄️ Database Explorer")
    st.markdown(
        """
        <div class="feature-box">
        Interactive database interface:
        <ul>
        <li>Browse schemas and tables</li>
        <li>Run custom SQL queries</li>
        <li>Export data (CSV, Excel)</li>
        <li><b>📊 Data visualizations</b></li>
        </ul>
        <br>
        <b>Actions:</b> Query data, analyze tables, visualize insights, download results
        </div>
        """,
        unsafe_allow_html=True,
    )

with col2:
    st.markdown("### 🤖 ML Dashboard")
    st.markdown(
        """
        <div class="feature-box">
        Machine Learning insights:
        <ul>
        <li>View model performance</li>
        <li>Analyze predictions</li>
        <li>Explore feature importance</li>
        </ul>
        <br>
        <b>Actions:</b> Review ML results and insights
        </div>
        """,
        unsafe_allow_html=True,
    )

with col3:
    st.markdown("### 🗺️ Interactive Analytics")
    st.markdown(
        """
        <div class="feature-box">
        Advanced data exploration:
        <ul>
        <li>Filter by ethnicity & demographics</li>
        <li>Test score analysis by race</li>
        <li>Interactive state/ZIP maps</li>
        <li><b>🎯 Custom dashboard builder</b></li>
        </ul>
        <br>
        <b>Actions:</b> Explore maps, filter data, build custom views
        </div>
        """,
        unsafe_allow_html=True,
    )

# System status section
st.markdown("---")
st.subheader("🔍 System Status")

try:
    from components.db_connector import DatabaseConnector

    db = DatabaseConnector()
    if db.test_connection():
        st.success("✅ Database connection is active")

        # Get basic stats
        try:
            schemas = db.list_schemas()
            st.info(f"📁 {len(schemas)} schema(s) available: {', '.join(schemas)}")
        except Exception as e:
            st.warning(f"Could not retrieve schema information: {e}")
    else:
        st.error("❌ Database connection failed - check configuration")

except Exception as e:
    st.error(f"❌ Database connection error: {str(e)}")
    st.info("💡 Make sure the database service is running: `docker-compose up db`")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; color: #666; padding: 1rem;">
    <p>Milestone 2 - Census & Education Data ETL Platform</p>
    <p>Built with Streamlit • PostgreSQL • Python</p>
    </div>
    """,
    unsafe_allow_html=True,
)
