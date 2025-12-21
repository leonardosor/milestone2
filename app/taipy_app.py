#!/usr/bin/env python3
"""
Taipy Application - Census & Education Data Analytics Platform
Main entry point for the Taipy-based application
"""

import os
import sys
from pathlib import Path
from taipy.gui import Gui, navigate, State
import pandas as pd

# Add components to path
sys.path.append(str(Path(__file__).parent))
from components.db_connector import get_db_connector

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
print(f"Loaded {len(assessment_df)} assessment records, {len(school_df)} schools, {len(census_df)} census records")

# ==================== Home Page ====================

home_md = """
# 🏠 Census & Education Data Platform

---

## 🎯 Project Motivation

This project provides a **production-ready analytics platform** for exploring the relationship between 
**demographic factors and academic performance** across the United States.

### Why This Matters
Understanding how demographics influence educational outcomes is crucial for:
- 📚 **Educational Policy**: Identifying achievement gaps across different populations
- 💰 **Resource Allocation**: Understanding how socioeconomic factors correlate with school performance
- 🗺️ **Geographic Insights**: Visualizing regional patterns in education and demographics
- 📊 **Data-Driven Decisions**: Providing actionable insights for educators and policymakers

### Data Sources
- **US Census Bureau API**: Demographics, household income, age distributions by ZIP code
- **Urban Institute Education Data API**: School directories, enrollment, and test scores
- **Geographic Data (TIGER/Line)**: US Census Bureau shapefiles for mapping

### Key Features
- Multi-year trend analysis of test scores by race/ethnicity and gender
- Interactive geographic maps showing school distributions
- Demographic breakdowns with income and population filters
- Custom dashboard builder for personalized analysis

---

## 📱 Platform Features

<|layout|columns=1 1 1|gap=20px|
### 🗺️ Interactive Analytics {: .card}

Advanced data exploration:
- Filter by ethnicity & demographics
- Test score analysis by race
- Interactive state/ZIP maps
- Custom dashboard builder

<|button|label=Go to Analytics|on_action=go_to_analytics|>
|

### 🗄️ Database Explorer {: .card}

Interactive database interface:
- Browse schemas and tables
- Run custom SQL queries
- Export data (CSV, Excel)
- Data visualizations

<|button|label=Go to Explorer|on_action=go_to_explorer|>
|

### 🤖 ML Dashboard {: .card}

Machine Learning insights:
- View model performance
- Analyze predictions
- Explore feature importance

<|button|label=Go to ML|on_action=go_to_ml|>
|>

---

## 🔍 System Status

<|{db_status}|>

<|{schema_info}|>
"""

# Database status
db_status = "✅ Database connection is active" if db.test_connection() else "❌ Database connection failed"
try:
    schemas = db.list_schemas()
    schema_info = f"📁 {len(schemas)} schema(s) available: {', '.join(schemas)}"
except:
    schema_info = "Could not retrieve schema information"

# ==================== Interactive Analytics Page ====================

analytics_md = """
# 🗺️ Interactive Analytics Dashboard

<|layout|columns=1|

## 📊 Test Score Analysis

<|layout|columns=1 1 1|gap=10px|
**Select Race/Ethnicity:**

<|{selected_races}|selector|lov={race_options}|multiple|dropdown|width=100%|>
|

**Select Years:**

<|{selected_years}|selector|lov={year_options}|multiple|dropdown|width=100%|>
|

**Score Type:**

<|{score_type}|toggle|lov=Math;Reading;Both|>
|>

<|button|label=🔍 Apply Filters|on_action=apply_analytics_filters|>

---

### 📚 Proficiency by Race/Ethnicity

<|{race_scores_chart}|chart|mode=lines+markers|>

---

### 👥 Demographic Explorer

<|layout|columns=1 1|gap=20px|
**Population Range:**

Min: <|{pop_min}|number|>  
Max: <|{pop_max}|number|>
|

**Income Level Focus:**

<|{income_filter}|selector|lov=All;High Income ($220K+);Upper Middle ($150K-$200K)|>
|>

<|button|label=🔍 Apply Demographic Filters|on_action=apply_demographic_filters|>

---

### 🥧 Ethnicity Breakdown

<|{ethnicity_chart}|chart|type=pie|>

---

### 🗺️ Geographic Maps

<|layout|columns=1 1 1|gap=10px|
**Select States:**

<|{selected_states}|selector|lov={state_options}|multiple|dropdown|>
|

**School Type:**

<|{selected_school_types}|selector|lov={school_type_options}|multiple|dropdown|>
|

**Enrollment Range:**

Min: <|{enrollment_min}|number|>  
Max: <|{enrollment_max}|number|>
|>

<|button|label=🔍 Apply Map Filters|on_action=apply_map_filters|>

<|{school_map}|chart|type=scattermapbox|>

|>

<|button|label=← Back to Home|on_action=go_home|>
"""

# Analytics page variables
selected_races = []
selected_years = []
score_type = "Math"
race_options = list(assessment_df['race'].dropna().unique()) if not assessment_df.empty else []
year_options = sorted(assessment_df['year'].dropna().unique().tolist()) if not assessment_df.empty else []

pop_min = 1000
pop_max = 50000
income_filter = "All"

selected_states = []
selected_school_types = []
state_options = sorted(school_df['state'].dropna().unique().tolist()) if not school_df.empty else []
school_type_options = school_df['school_type'].dropna().unique().tolist() if not school_df.empty else []
enrollment_min = 0
enrollment_max = 5000

race_scores_chart = {}
ethnicity_chart = {}
school_map = {}

# ==================== Database Explorer Page ====================

explorer_md = """
# 🗄️ Database Explorer

<|layout|columns=1|

## Browse Schemas and Tables

**Select Schema:**

<|{selected_schema}|selector|lov={schema_list}|>

**Select Table:**

<|{selected_table}|selector|lov={table_list}|>

<|button|label=Load Table|on_action=load_table_data|>

---

### Table Preview

<|{table_data}|table|>

<|button|label=📥 Download CSV|on_action=download_csv|>

---

## Custom SQL Query

<|{sql_query}|input|multiline|label=Enter SQL Query|>

<|button|label=▶️ Execute Query|on_action=execute_query|>

### Query Results

<|{query_results}|table|>

|>

<|button|label=← Back to Home|on_action=go_home|>
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
# 🤖 ML Model Dashboard

<|layout|columns=1|

## Model Overview

### 🚧 Under Construction

This page will be expanded to include:
- Model performance metrics
- Feature importance visualizations
- Prediction results
- Model comparison tools

---

## Available Notebooks

The following Jupyter notebooks are available in the `notebooks/` directory:

1. **supervised.ipynb**: Supervised learning models
   - Linear Regression
   - Random Forest
   - XGBoost
   - Neural Networks (MLP)

2. **unsupervised.ipynb**: Unsupervised learning models
   - PCA (Principal Component Analysis)
   - K-Means Clustering
   - Gaussian Mixture Models

---

## Model Artifacts

Model artifacts (trained models, parameters, metrics) are stored in:
- `supervised/`: Supervised model results
- `unsupervised/`: Unsupervised model results

|>

<|button|label=← Back to Home|on_action=go_home|>
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
        filtered = filtered[filtered['race'].isin(state.selected_races)]
    
    if state.selected_years:
        filtered = filtered[filtered['year'].isin(state.selected_years)]
    
    if not filtered.empty:
        # Aggregate scores by race
        race_scores = filtered.groupby('race').agg({
            'math_prof_mid': 'mean',
            'read_prof_mid': 'mean',
        }).reset_index()
        
        # Create chart data
        state.race_scores_chart = {
            'data': [
                {
                    'x': race_scores['race'].tolist(),
                    'y': race_scores['math_prof_mid' if state.score_type in ['Math', 'Both'] else 'read_prof_mid'].tolist(),
                    'type': 'bar',
                    'name': state.score_type
                }
            ],
            'layout': {
                'title': f'{state.score_type} Proficiency by Race/Ethnicity',
                'xaxis': {'title': 'Race/Ethnicity'},
                'yaxis': {'title': 'Proficiency (%)'}
            }
        }

def apply_demographic_filters(state: State):
    """Apply demographic filters"""
    filtered = census_df[
        (census_df['total_pop'] >= state.pop_min) & 
        (census_df['total_pop'] <= state.pop_max)
    ].copy()
    
    if not filtered.empty:
        # Calculate ethnicity totals
        filtered['white_total'] = filtered['white_males_10_14'].fillna(0) + filtered['white_females_10_14'].fillna(0)
        filtered['black_total'] = filtered['black_males_10_14'].fillna(0) + filtered['black_females_10_14'].fillna(0)
        filtered['hispanic_total'] = filtered['hispanic_males_10_14'].fillna(0) + filtered['hispanic_females_10_14'].fillna(0)
        
        # Create pie chart
        state.ethnicity_chart = {
            'data': [{
                'values': [
                    filtered['white_total'].sum(),
                    filtered['black_total'].sum(),
                    filtered['hispanic_total'].sum()
                ],
                'labels': ['White', 'Black', 'Hispanic'],
                'type': 'pie'
            }],
            'layout': {'title': 'Ethnicity Breakdown (Ages 10-14)'}
        }

def apply_map_filters(state: State):
    """Apply map filters for school locations"""
    filtered = school_df.copy()
    
    if state.selected_states:
        filtered = filtered[filtered['state'].isin(state.selected_states)]
    
    if state.selected_school_types:
        filtered = filtered[filtered['school_type'].isin(state.selected_school_types)]
    
    filtered = filtered[
        (filtered['enrollment'] >= state.enrollment_min) & 
        (filtered['enrollment'] <= state.enrollment_max)
    ]
    
    # Limit to first 1000 for performance
    if len(filtered) > 1000:
        filtered = filtered.sample(1000)
    
    if not filtered.empty:
        state.school_map = {
            'data': [{
                'type': 'scattermapbox',
                'lat': filtered['latitude'].tolist(),
                'lon': filtered['longitude'].tolist(),
                'mode': 'markers',
                'marker': {'size': 8, 'color': 'blue'},
                'text': filtered['school_name'].tolist()
            }],
            'layout': {
                'mapbox': {
                    'style': 'open-street-map',
                    'center': {'lat': 39.8283, 'lon': -98.5795},
                    'zoom': 3
                },
                'height': 600
            }
        }

# ==================== Explorer Actions ====================

def load_table_data(state: State):
    """Load table data from selected schema and table"""
    if state.selected_schema and state.selected_table:
        try:
            state.table_data = db.get_table_data(
                state.selected_schema, 
                state.selected_table, 
                limit=1000
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

if __name__ == "__main__":
    # Get port from environment variable (for Railway/Heroku) or default to 5000
    port = int(os.getenv("PORT", "5000"))
    
    gui = Gui(pages=pages)
    gui.run(
        title="Census & Education Data Platform",
        host="0.0.0.0",  # Bind to all interfaces for cloud deployment
        port=port,
        dark_mode=False,
        use_reloader=True
    )
