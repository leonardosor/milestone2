#!/usr/bin/env python3
"""
Home Page
Census & Education Data Analytics Platform
"""

import sys
from pathlib import Path

import streamlit as st

# Add ETL modules to path
sys.path.append(str(Path(__file__).parent / "etl_modules"))

# Page configuration
st.set_page_config(
    page_title="Home - Census & Education Analytics",
    page_icon="🏠",
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
    '<div class="main-header">🏠 Census & Education Data Platform</div>',
    unsafe_allow_html=True,
)
st.markdown("---")

# Sidebar navigation
st.sidebar.title("🧭 Navigation")
st.sidebar.info(
    """
    **This application provides:**
    - Interactive analytics & visualizations
    - Database exploration
    - ML model insights
    """
)

st.sidebar.markdown("---")
st.sidebar.markdown("### Quick Links")
st.sidebar.markdown("- 🗺️ [Interactive Analytics](/0_Interactive_Analytics)")
st.sidebar.markdown("- 🗄️ [Database Explorer](/2_Database_Explorer)")
st.sidebar.markdown("- 🤖 [ML Dashboard](/3_ML_Dashboard)")


# Main page content - Feature overview
st.subheader("Welcome to the Data Platform")

# Project Motivation Section
st.markdown(
    """
### 🎯 Project Motivation

This project provides a **production-ready analytics platform** for exploring the relationship between
**demographic factors and academic performance** across the United States.

#### Why This Matters
Understanding how demographics influence educational outcomes is crucial for:
- 📚 **Educational Policy**: Identifying achievement gaps across different populations
- 💰 **Resource Allocation**: Understanding how socioeconomic factors correlate with school performance
- 🗺️ **Geographic Insights**: Visualizing regional patterns in education and demographics
- 📊 **Data-Driven Decisions**: Providing actionable insights for educators and policymakers

#### Data Sources
- **US Census Bureau API**: Demographics, household income, age distributions by ZIP code
- **Urban Institute Education Data API**: School directories, enrollment, and test scores
- **Geographic Data (TIGER/Line)**: US Census Bureau shapefiles for mapping

#### Key Features
- Multi-year trend analysis of test scores by race/ethnicity and gender
- Interactive geographic maps showing school distributions
- Demographic breakdowns with income and population filters
- Custom dashboard builder for personalized analysis
"""
)

st.markdown("---")
st.subheader("📱 Platform Features")

col1, col2, col3 = st.columns(3)

with col1:
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

with col2:
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

with col3:
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
