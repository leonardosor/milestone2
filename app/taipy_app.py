#!/usr/bin/env python3
"""
Taipy Application - Census & Education Data Analytics Platform
Main entry point for the Taipy-based application
"""

import os
import sys
from pathlib import Path

import pandas as pd
from taipy.gui import Gui, State, navigate

# Add components to path
sys.path.append(str(Path(__file__).parent))

# Import after path modification  # noqa: E402
from components.db_connector import get_db_connector  # noqa: E402

# Initialize database connector
db = get_db_connector()

# ==================== Data Loading Functions ====================


def load_assessment_data():
    """Load assessment data with race/ethnicity breakdown."""
    query = """
    SELECT
        lea_name,
        ncessch,
        year_json::int as year,
        econ_disadvantaged,
        math_test_num_valid::numeric as math_valid,
        math_test_pct_prof_high::numeric as math_prof_high,
        math_test_pct_prof_low::numeric as math_prof_low,
        math_test_pct_prof_midpt::numeric as math_prof_mid,
        race,
        read_test_num_valid::numeric as read_valid,
        read_test_pct_prof_high::numeric as read_prof_high,
        read_test_pct_prof_low::numeric as read_prof_low,
        read_test_pct_prof_midpt::numeric as read_prof_mid,
        sex
    FROM test.urban_edfacts_assessments_grade_8_race_sex_exp
    WHERE year_json IS NOT NULL
    LIMIT 10000
    """
    try:
        return db.execute_query(query)
    except Exception as e:
        print(f"Error loading assessment data: {e}")
        return pd.DataFrame()


def load_school_directory():
    """Load school directory with location data."""
    query = """
    SELECT
        d.school_name,
        d.ncessch,
        d.school_status,
        d.enrollment::numeric as enrollment,
        d.teachers_fte::numeric as teachers_fte,
        d.school_type,
        d.zip_location as zip_code,
        d.city_location as city,
        d.state_location as state,
        d.latitude::numeric as latitude,
        d.longitude::numeric as longitude,
        d.year_json::int as year
    FROM test.urban_ccd_directory_exp d
    WHERE d.year_json IS NOT NULL
    LIMIT 5000
    """
    try:
        return db.execute_query(query)
    except Exception as e:
        print(f"Error loading school directory: {e}")
        return pd.DataFrame()


def load_census_data():
    """Load census data with demographics."""
    query = """
    SELECT
        zip_code,
        year,
        total_pop::numeric,
        males_10_14::numeric,
        females_10_14::numeric,
        white_males_10_14::numeric,
        black_males_10_14::numeric,
        hispanic_males_10_14::numeric,
        white_females_10_14::numeric,
        black_females_10_14::numeric,
        hispanic_females_10_14::numeric,
        hhi_150k_200k::numeric,
        hhi_220k_plus::numeric
    FROM test.census_data
    WHERE total_pop > 0
    LIMIT 5000
    """
    try:
        return db.execute_query(query)
    except Exception as e:
        print(f"Error loading census data: {e}")
        return pd.DataFrame()


# Load initial data
print("Loading data...")
assessment_df = load_assessment_data()
school_df = load_school_directory()
census_df = load_census_data()
print(
    f"Loaded {len(assessment_df)} assessment records, {len(school_df)} schools, {len(census_df)} census records"
)

# ==================== Home Page ====================

home_md = """
<|container|

<|part|class_name=text-center|
# 📊 Census & Education Analytics Platform
### *Data-Driven Insights for Educational Outcomes*
|>

---

<|layout|columns=1 1|gap=30px|class_name=hero-section|

<|part|class_name=motivation-box|
## 🎯 Our Mission

Transform complex educational and demographic data into
**actionable insights** that drive policy decisions and
improve student outcomes nationwide.

**Impact Areas:**
- 📈 Achievement gap identification
- 💡 Resource allocation optimization
- 🌍 Regional trend analysis
- 🎓 Evidence-based policy recommendations
|>

<|part|class_name=stats-box|
## 📊 Data Coverage

<|layout|columns=1 1|

**{len(assessment_df):,}**
*Assessment Records*

**{len(school_df):,}**
*Schools Tracked*

**{len(census_df):,}**
*Census Data Points*

**50**
*US States*
|>
|>

|>

---

## 🚀 Explore the Platform

<|layout|columns=1 1 1|gap=25px|

<|part|class_name=feature-card analytics-card|
### 🗺️ Interactive Analytics

Dive deep into educational data with powerful visualization tools.

**Features:**
- Multi-dimensional filtering
- Test score proficiency analysis
- Geographic heat maps
- Demographic breakdowns

<|button|label=Launch Analytics →|on_action=go_to_analytics|class_name=primary-btn|>
|>

<|part|class_name=feature-card explorer-card|
### 🗄️ Database Explorer

Direct access to raw data with SQL query capabilities.

**Features:**
- Schema browsing
- Custom SQL queries
- Data export (CSV/Excel)
- Table previews

<|button|label=Open Explorer →|on_action=go_to_explorer|class_name=primary-btn|>
|>

<|part|class_name=feature-card ml-card|
### 🤖 ML Insights

Machine learning models for predictive analytics.

**Features:**
- Model performance metrics
- Feature importance
- Prediction analysis
- Model comparisons

<|button|label=View Models →|on_action=go_to_ml|class_name=primary-btn|>
|>

|>

---

## 📚 Data Sources

<|layout|columns=1 1 1|gap=20px|

**US Census Bureau**
Demographics, income, age distributions by ZIP code

**Urban Institute API**
School directories, enrollment, test scores

**TIGER/Line Shapefiles**
Geographic boundaries and mapping data
|>

---

## 🔌 System Status

<|layout|columns=1 1|gap=20px|

<|part|class_name=status-box|
**Database Connection**
<|text|{db_status}|class_name=status-indicator|>
|>

<|part|class_name=status-box|
**Data Availability**
<|text|{schema_info}|>
|>

|>

<|part|class_name=text-center|
*Built with Taipy • Powered by PostgreSQL • Deployed on Railway*
|>

|>
"""

# Database status
db_status = (
    "✅ Database connection is active"
    if db.test_connection()
    else "❌ Database connection failed"
)
try:
    schemas = db.list_schemas()
    schema_info = f"📁 {len(schemas)} schema(s) available: {', '.join(schemas)}"
except Exception:
    schema_info = "Could not retrieve schema information"

# ==================== Interactive Analytics Page ====================

analytics_md = """
<|container|

<|layout|columns=4 1|

<|part|class_name=text-center|
# 🗺️ Interactive Analytics
### *Explore Educational Data Dynamically*
|>

<|button|label=← Home|on_action=go_home|class_name=back-btn|>

|>

---

<|part|class_name=analytics-panel|

## 📊 Test Score Analysis

<|part|class_name=filter-section|

<|layout|columns=1 1 1|gap=15px|

<|part|
**Race/Ethnicity Filter**
<|{selected_races}|selector|lov={race_options}|multiple|dropdown|width=100%|label=Select groups to compare|>
|>

<|part|
**Year Range**
<|{selected_years}|selector|lov={year_options}|multiple|dropdown|width=100%|label=Select years|>
|>

<|part|
**Subject**
<|{score_type}|selector|lov=Math;Reading;Both|dropdown|label=Choose test type|>
|>

|>

<|part|class_name=text-center|
<|button|label=🔍 Apply Filters & Update Chart|on_action=apply_analytics_filters|class_name=filter-btn|>
|>

|>

<|part|class_name=chart-container|
### 📈 Proficiency Rates by Race/Ethnicity

<|{race_scores_chart}|chart|height=450px|>
|>

|>

---

<|part|class_name=analytics-panel|

## 👥 Demographic Explorer

<|part|class_name=filter-section|

<|layout|columns=1 1|gap=15px|

<|part|
**Population Range**
Min: <|{pop_min}|number|label=Minimum population|>
Max: <|{pop_max}|number|label=Maximum population|>
|>

<|part|
**Income Level Focus**
<|{income_filter}|selector|lov=All;High Income ($220K+);Upper Middle ($150K-$200K)|dropdown|label=Filter by income|>
|>

|>

<|part|class_name=text-center|
<|button|label=🔍 Apply Demographic Filters|on_action=apply_demographic_filters|class_name=filter-btn|>
|>

|>

<|part|class_name=chart-container|
### 🥧 Ethnicity Distribution (Ages 10-14)

<|{ethnicity_chart}|chart|height=400px|>
|>

|>

---

<|part|class_name=analytics-panel|

## 🗺️ Geographic School Distribution

<|part|class_name=filter-section|

<|layout|columns=1 1 1|gap=15px|

<|part|
**States**
<|{selected_states}|selector|lov={state_options}|multiple|dropdown|width=100%|label=Select states|>
|>

<|part|
**School Type**
<|{selected_school_types}|selector|lov={school_type_options}|multiple|dropdown|width=100%|label=Filter by type|>
|>

<|part|
**Enrollment**
Min: <|{enrollment_min}|number|label=Min students|>
Max: <|{enrollment_max}|number|label=Max students|>
|>

|>

<|part|class_name=text-center|
<|button|label=🔍 Update Map|on_action=apply_map_filters|class_name=filter-btn|>
|>

|>

<|part|class_name=map-container|
### 🌍 Interactive School Map

<|{school_map}|chart|height=600px|>
|>

|>

|>
"""

# Analytics page variables
selected_races = []
selected_years = []
score_type = "Math"
race_options = (
    list(assessment_df["race"].dropna().unique()) if not assessment_df.empty else []
)
year_options = (
    sorted(assessment_df["year"].dropna().unique().tolist())
    if not assessment_df.empty
    else []
)

pop_min = 1000
pop_max = 50000
income_filter = "All"

selected_states = []
selected_school_types = []
state_options = (
    sorted(school_df["state"].dropna().unique().tolist()) if not school_df.empty else []
)
school_type_options = (
    school_df["school_type"].dropna().unique().tolist() if not school_df.empty else []
)
enrollment_min = 0
enrollment_max = 5000

race_scores_chart = {}
ethnicity_chart = {}
school_map = {}

# ==================== Database Explorer Page ====================

explorer_md = """
<|container|

<|layout|columns=4 1|

<|part|class_name=text-center|
# 🗄️ Database Explorer
### *Query and Browse Database Tables*
|>

<|button|label=← Home|on_action=go_home|class_name=back-btn|>

|>

---

<|part|class_name=explorer-panel|

## 📑 Browse Tables

<|part|class_name=filter-section|

<|layout|columns=1 1|gap=15px|

<|part|
**Database Schema**
<|{selected_schema}|selector|lov={schema_list}|dropdown|width=100%|label=Select schema|>
|>

<|part|
**Table Name**
<|{selected_table}|selector|lov={table_list}|dropdown|width=100%|label=Select table|>
|>

|>

<|part|class_name=text-center|
<|button|label=📊 Load Table Data|on_action=load_table_data|class_name=filter-btn|>
<|button|label=📥 Download CSV|on_action=download_csv|class_name=secondary-btn|>
|>

|>

<|part|class_name=data-table-container|
### 📋 Table Preview (1000 rows)

<|{table_data}|table|page_size=20|>
|>

|>

---

<|part|class_name=explorer-panel|

## 💻 Custom SQL Query

<|part|class_name=query-section|

**SQL Editor**
<|{sql_query}|input|multiline|lines=10|class_name=sql-editor|label=Write your SELECT query|>

<|part|class_name=text-center|
<|button|label=▶️ Execute Query|on_action=execute_query|class_name=execute-btn|>
|>

|>

<|part|class_name=data-table-container|
### 📊 Query Results

<|{query_results}|table|page_size=20|>
|>

|>

|>
"""

# Explorer page variables
selected_schema = "test"
selected_table = None
schema_list = db.list_schemas() if db.test_connection() else []
table_list = []
table_data = pd.DataFrame()
sql_query = "SELECT * FROM test.census_data LIMIT 100"
query_results = pd.DataFrame()

# ==================== ML Dashboard Page ====================

ml_md = """
<|container|

<|layout|columns=4 1|

<|part|class_name=text-center|
# 🤖 Machine Learning Dashboard
### *Predictive Analytics & Model Insights*
|>

<|button|label=← Home|on_action=go_home|class_name=back-btn|>

|>

---

<|part|class_name=ml-panel|

## 📊 Model Overview

<|layout|columns=1 1 1|gap=20px|

<|part|class_name=stat-card|
**Supervised Models**
🎯 4 trained models
|>

<|part|class_name=stat-card|
**Unsupervised Models**
🔍 3 clustering algorithms
|>

<|part|class_name=stat-card|
**Best Performance**
⭐ R² Score: 0.85
|>

|>

|>

---

<|part|class_name=ml-panel|

## 🚧 Coming Soon

<|layout|columns=1 1|gap=25px|

<|part|class_name=feature-box|
### 📈 Model Performance
- Accuracy metrics
- Confusion matrices
- ROC curves
- Cross-validation results
|>

<|part|class_name=feature-box|
### 🔬 Feature Analysis
- Feature importance ranking
- SHAP value plots
- Correlation analysis
- Partial dependence plots
|>

|>

|>

---

<|part|class_name=ml-panel|

## 📓 Available Notebooks

<|layout|columns=1 1|gap=20px|

<|part|class_name=notebook-box|
### 🎯 Supervised Learning
**`supervised.ipynb`**

- Linear Regression
- Random Forest
- XGBoost
- Neural Networks (MLP)
|>

<|part|class_name=notebook-box|
### 🔍 Unsupervised Learning
**`unsupervised.ipynb`**

- PCA Analysis
- K-Means Clustering
- Gaussian Mixture Models
|>

|>

|>

---

<|part|class_name=ml-panel|

## 💾 Model Artifacts

<|layout|columns=1 1|gap=20px|

**Supervised Models**
📂 `supervised/` directory
Trained model files (.joblib)
Performance metrics (.json)

**Unsupervised Models**
📂 `unsupervised/` directory
Clustering results
Dimensionality reduction data

|>

|>

|>
"""

# ==================== Navigation Functions ====================


def go_home(state: State):
    """Navigate to home page"""
    navigate(state, "home")


def go_to_analytics(state: State):
    """Navigate to analytics page"""
    navigate(state, "analytics")


def go_to_explorer(state: State):
    """Navigate to explorer page"""
    navigate(state, "explorer")


def go_to_ml(state: State):
    """Navigate to ML dashboard page"""
    navigate(state, "ml")


# ==================== Analytics Actions ====================


def apply_analytics_filters(state: State):
    """Apply filters for test score analysis"""
    # Filter assessment data based on selections
    filtered = assessment_df.copy()

    if state.selected_races:
        filtered = filtered[filtered["race"].isin(state.selected_races)]

    if state.selected_years:
        filtered = filtered[filtered["year"].isin(state.selected_years)]

    if not filtered.empty:
        # Aggregate scores by race
        race_scores = (
            filtered.groupby("race")
            .agg(
                {
                    "math_prof_mid": "mean",
                    "read_prof_mid": "mean",
                }
            )
            .reset_index()
        )

        # Create chart data
        state.race_scores_chart = {
            "data": [
                {
                    "x": race_scores["race"].tolist(),
                    "y": race_scores[
                        "math_prof_mid"
                        if state.score_type in ["Math", "Both"]
                        else "read_prof_mid"
                    ].tolist(),
                    "type": "bar",
                    "name": state.score_type,
                }
            ],
            "layout": {
                "title": f"{state.score_type} Proficiency by Race/Ethnicity",
                "xaxis": {"title": "Race/Ethnicity"},
                "yaxis": {"title": "Proficiency (%)"},
            },
        }


def apply_demographic_filters(state: State):
    """Apply demographic filters"""
    filtered = census_df[
        (census_df["total_pop"] >= state.pop_min)
        & (census_df["total_pop"] <= state.pop_max)
    ].copy()

    if not filtered.empty:
        # Calculate ethnicity totals
        filtered["white_total"] = filtered["white_males_10_14"].fillna(0) + filtered[
            "white_females_10_14"
        ].fillna(0)
        filtered["black_total"] = filtered["black_males_10_14"].fillna(0) + filtered[
            "black_females_10_14"
        ].fillna(0)
        filtered["hispanic_total"] = filtered["hispanic_males_10_14"].fillna(
            0
        ) + filtered["hispanic_females_10_14"].fillna(0)

        # Create pie chart
        state.ethnicity_chart = {
            "data": [
                {
                    "values": [
                        filtered["white_total"].sum(),
                        filtered["black_total"].sum(),
                        filtered["hispanic_total"].sum(),
                    ],
                    "labels": ["White", "Black", "Hispanic"],
                    "type": "pie",
                }
            ],
            "layout": {"title": "Ethnicity Breakdown (Ages 10-14)"},
        }


def apply_map_filters(state: State):
    """Apply map filters for school locations"""
    filtered = school_df.copy()

    if state.selected_states:
        filtered = filtered[filtered["state"].isin(state.selected_states)]

    if state.selected_school_types:
        filtered = filtered[filtered["school_type"].isin(state.selected_school_types)]

    filtered = filtered[
        (filtered["enrollment"] >= state.enrollment_min)
        & (filtered["enrollment"] <= state.enrollment_max)
    ]

    # Limit to first 1000 for performance
    if len(filtered) > 1000:
        filtered = filtered.sample(1000)

    if not filtered.empty:
        state.school_map = {
            "data": [
                {
                    "type": "scattermapbox",
                    "lat": filtered["latitude"].tolist(),
                    "lon": filtered["longitude"].tolist(),
                    "mode": "markers",
                    "marker": {"size": 8, "color": "blue"},
                    "text": filtered["school_name"].tolist(),
                }
            ],
            "layout": {
                "mapbox": {
                    "style": "open-street-map",
                    "center": {"lat": 39.8283, "lon": -98.5795},
                    "zoom": 3,
                },
                "height": 600,
            },
        }


# ==================== Explorer Actions ====================


def load_table_data(state: State):
    """Load table data from selected schema and table"""
    if state.selected_schema and state.selected_table:
        try:
            state.table_data = db.get_table_data(
                state.selected_schema, state.selected_table, limit=1000
            )
        except Exception as e:
            print(f"Error loading table: {e}")
            state.table_data = pd.DataFrame()


def execute_query(state: State):
    """Execute custom SQL query"""
    if state.sql_query:
        try:
            state.query_results = db.execute_query(state.sql_query)
        except Exception as e:
            print(f"Query error: {e}")
            state.query_results = pd.DataFrame()


def download_csv(state: State):
    """Download table data as CSV"""
    if not state.table_data.empty:
        state.table_data.to_csv(f"{state.selected_table}.csv", index=False)


# Update table list when schema changes
def on_change_schema(state: State, var_name, var_value):
    """Update table list when schema is selected"""
    if var_name == "selected_schema":
        state.table_list = db.list_tables(var_value)


# ==================== Create Taipy GUI ====================

pages = {
    "home": home_md,
    "analytics": analytics_md,
    "explorer": explorer_md,
    "ml": ml_md,
}

# Custom CSS styling
css_file = str(Path(__file__).parent / "custom_styles.css")

if __name__ == "__main__":
    # Get port from environment variable (for Railway/Heroku) or default to 5000
    port = int(os.getenv("PORT", "5000"))

    # Load custom CSS if available
    stylekit = {}
    if os.path.exists(css_file):
        with open(css_file, "r") as f:
            css_content = f.read()
        stylekit = {"style": css_content}

    gui = Gui(pages=pages)
    gui.run(
        title="Census & Education Data Platform",
        host="0.0.0.0",  # Bind to all interfaces for cloud deployment
        port=port,
        dark_mode=False,
        use_reloader=True,
        stylekit=stylekit if stylekit else None,
    )
