#!/usr/bin/env python3
"""
Interactive Analytics Page

Advanced data exploration with:
- Filters for ethnicity, demographics, and test scores
- Interactive maps showing states and zip codes
- Customizable charts and graphs
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# Add components to path
sys.path.append(str(Path(__file__).parent.parent))

from components.db_connector import get_db_connector  # noqa: E402

# Page configuration
st.set_page_config(
    page_title="Interactive Analytics",
    page_icon="🗺️",
    layout="wide",
)

st.title("🗺️ Interactive Analytics Dashboard")
st.markdown("Explore demographic data with filters and interactive maps")

# Initialize database connector
db = get_db_connector()

# Test connection
if not db.test_connection():
    st.error("❌ Cannot connect to database. Check your configuration.")
    st.stop()


@st.cache_data(ttl=300)
def load_assessment_data():
    """Load assessment data with race/ethnicity breakdown."""
    query = """
    SELECT
        lea_name,
        ncessch,
        year_json::int as year,
        econ_disadvantaged,
        grade,
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
    """
    try:
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Error loading assessment data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
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
    """
    try:
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Error loading school directory: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
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
    """
    try:
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Error loading census data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_location_data():
    """Load location data for mapping."""
    query = """
    SELECT
        zip,
        state,
        state_fips,
        county_fips,
        latitude::numeric,
        longitude::numeric
    FROM test.location_data
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    try:
        return db.execute_query(query)
    except Exception as e:
        st.error(f"Error loading location data: {e}")
        return pd.DataFrame()


# Load all data
with st.spinner("Loading data..."):
    assessment_df = load_assessment_data()
    school_df = load_school_directory()
    census_df = load_census_data()
    location_df = load_location_data()

# Sidebar Filters
st.sidebar.header("🎛️ Filters")

# Create tabs for different analysis types
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "📊 Test Score Analysis",
        "👥 Demographic Explorer",
        "🗺️ Geographic Maps",
        "📈 Trend Analysis",
        "🎯 Custom Dashboard",
    ]
)

# ==================== TAB 1: Test Score Analysis ====================
with tab1:
    st.subheader("📊 Test Score Analysis by Demographics")

    if not assessment_df.empty:
        col1, col2, col3 = st.columns(3)

        with col1:
            # Race/Ethnicity filter
            race_options = assessment_df["race"].dropna().unique().tolist()
            if race_options:
                selected_races = st.multiselect(
                    "Select Race/Ethnicity",
                    options=sorted(race_options),
                    default=race_options[:5]
                    if len(race_options) >= 5
                    else race_options,
                    key="race_filter_1",
                )
            else:
                selected_races = []

        with col2:
            # Year filter
            years = sorted(assessment_df["year"].dropna().unique())
            if len(years) > 0:
                selected_years = st.multiselect(
                    "Select Years",
                    options=years,
                    default=[max(years)] if years else [],
                    key="year_filter_1",
                )
            else:
                selected_years = []

        with col3:
            # Score type selection
            score_type = st.radio(
                "Score Type",
                ["Math", "Reading", "Both"],
                horizontal=True,
                key="score_type_1",
            )

        # Filter data
        filtered_df = assessment_df.copy()
        if selected_races:
            filtered_df = filtered_df[filtered_df["race"].isin(selected_races)]
        if selected_years:
            filtered_df = filtered_df[filtered_df["year"].isin(selected_years)]

        if not filtered_df.empty:
            # Aggregate scores by race
            race_scores = (
                filtered_df.groupby("race")
                .agg(
                    {
                        "math_prof_mid": "mean",
                        "read_prof_mid": "mean",
                        "math_valid": "sum",
                        "read_valid": "sum",
                    }
                )
                .reset_index()
            )
            race_scores.columns = [
                "Race",
                "Math Proficiency (%)",
                "Reading Proficiency (%)",
                "Math Test Count",
                "Reading Test Count",
            ]

            col_a, col_b = st.columns(2)

            with col_a:
                st.markdown("### 📚 Proficiency by Race/Ethnicity")

                if score_type in ["Math", "Both"]:
                    fig_math = px.bar(
                        race_scores.sort_values("Math Proficiency (%)", ascending=True),
                        y="Race",
                        x="Math Proficiency (%)",
                        orientation="h",
                        title="Math Proficiency by Race/Ethnicity",
                        color="Math Proficiency (%)",
                        color_continuous_scale="Blues",
                    )
                    fig_math.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig_math, use_container_width=True)

            with col_b:
                if score_type in ["Reading", "Both"]:
                    fig_read = px.bar(
                        race_scores.sort_values(
                            "Reading Proficiency (%)", ascending=True
                        ),
                        y="Race",
                        x="Reading Proficiency (%)",
                        orientation="h",
                        title="Reading Proficiency by Race/Ethnicity",
                        color="Reading Proficiency (%)",
                        color_continuous_scale="Greens",
                    )
                    fig_read.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig_read, use_container_width=True)

            # Combined comparison chart
            st.markdown("### 📊 Math vs Reading Proficiency Comparison")

            fig_comparison = go.Figure()
            fig_comparison.add_trace(
                go.Bar(
                    name="Math Proficiency",
                    x=race_scores["Race"],
                    y=race_scores["Math Proficiency (%)"],
                    marker_color="steelblue",
                )
            )
            fig_comparison.add_trace(
                go.Bar(
                    name="Reading Proficiency",
                    x=race_scores["Race"],
                    y=race_scores["Reading Proficiency (%)"],
                    marker_color="forestgreen",
                )
            )
            fig_comparison.update_layout(
                barmode="group",
                title="Math vs Reading Proficiency by Race/Ethnicity",
                xaxis_title="Race/Ethnicity",
                yaxis_title="Proficiency (%)",
                height=450,
            )
            st.plotly_chart(fig_comparison, use_container_width=True)

            # Scatter plot for relationship
            st.markdown("### 🔗 Relationship Between Math and Reading Scores")

            fig_scatter = px.scatter(
                race_scores,
                x="Math Proficiency (%)",
                y="Reading Proficiency (%)",
                size="Math Test Count",
                color="Race",
                hover_name="Race",
                title="Math vs Reading Proficiency (bubble size = test count)",
            )
            fig_scatter.update_layout(height=450)
            st.plotly_chart(fig_scatter, use_container_width=True)

            # Data table
            with st.expander("📋 View Detailed Data"):
                st.dataframe(race_scores, use_container_width=True)
        else:
            st.warning(
                "No data matches the selected filters. Try adjusting your selections."
            )
    else:
        st.warning("Assessment data not available.")


# ==================== TAB 2: Demographic Explorer ====================
with tab2:
    st.subheader("👥 Demographic Explorer")

    if not census_df.empty:
        # Calculate ethnicity totals
        census_df["white_total"] = census_df["white_males_10_14"].fillna(0) + census_df[
            "white_females_10_14"
        ].fillna(0)
        census_df["black_total"] = census_df["black_males_10_14"].fillna(0) + census_df[
            "black_females_10_14"
        ].fillna(0)
        census_df["hispanic_total"] = census_df["hispanic_males_10_14"].fillna(
            0
        ) + census_df["hispanic_females_10_14"].fillna(0)
        census_df["total_10_14"] = census_df["males_10_14"].fillna(0) + census_df[
            "females_10_14"
        ].fillna(0)

        col1, col2 = st.columns(2)

        with col1:
            # Population filter
            pop_range = st.slider(
                "Population Range",
                min_value=int(census_df["total_pop"].min()),
                max_value=min(int(census_df["total_pop"].max()), 100000),
                value=(1000, 50000),
                key="pop_filter_2",
            )

        with col2:
            # Income filter
            income_filter = st.selectbox(
                "Income Level Focus",
                ["All", "High Income ($220K+)", "Upper Middle ($150K-$200K)"],
                key="income_filter_2",
            )

        # Filter by population
        demo_filtered = census_df[
            (census_df["total_pop"] >= pop_range[0])
            & (census_df["total_pop"] <= pop_range[1])
        ]

        if not demo_filtered.empty:
            # Ethnicity breakdown pie chart
            st.markdown("### 🥧 Ethnicity Breakdown (Ages 10-14)")

            ethnicity_totals = pd.DataFrame(
                {
                    "Ethnicity": ["White", "Black", "Hispanic", "Other"],
                    "Population": [
                        demo_filtered["white_total"].sum(),
                        demo_filtered["black_total"].sum(),
                        demo_filtered["hispanic_total"].sum(),
                        demo_filtered["total_10_14"].sum()
                        - demo_filtered["white_total"].sum()
                        - demo_filtered["black_total"].sum()
                        - demo_filtered["hispanic_total"].sum(),
                    ],
                }
            )
            ethnicity_totals = ethnicity_totals[ethnicity_totals["Population"] > 0]

            col_a, col_b = st.columns(2)

            with col_a:
                fig_pie = px.pie(
                    ethnicity_totals,
                    values="Population",
                    names="Ethnicity",
                    title="Children Ages 10-14 by Ethnicity",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig_pie.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig_pie, use_container_width=True)

            with col_b:
                # Gender breakdown
                gender_data = pd.DataFrame(
                    {
                        "Gender": ["Male", "Female"],
                        "Population": [
                            demo_filtered["males_10_14"].sum(),
                            demo_filtered["females_10_14"].sum(),
                        ],
                    }
                )

                fig_gender = px.pie(
                    gender_data,
                    values="Population",
                    names="Gender",
                    title="Children Ages 10-14 by Gender",
                    color_discrete_map={"Male": "#4A90D9", "Female": "#E94B87"},
                )
                fig_gender.update_traces(
                    textposition="inside", textinfo="percent+label"
                )
                st.plotly_chart(fig_gender, use_container_width=True)

            # Income distribution
            st.markdown("### 💰 Income Distribution Analysis")

            # Calculate income percentages
            demo_filtered["pct_high_income"] = (
                demo_filtered["hhi_220k_plus"] / demo_filtered["total_pop"] * 100
            ).fillna(0)
            demo_filtered["pct_upper_mid"] = (
                demo_filtered["hhi_150k_200k"] / demo_filtered["total_pop"] * 100
            ).fillna(0)

            fig_income = px.histogram(
                demo_filtered,
                x="pct_high_income",
                nbins=30,
                title="Distribution of High Income Households ($220K+) by ZIP Code",
                labels={"pct_high_income": "% High Income Households"},
                color_discrete_sequence=["#2E86AB"],
            )
            fig_income.update_layout(
                xaxis_title="% of Households Earning $220K+",
                yaxis_title="Number of ZIP Codes",
            )
            st.plotly_chart(fig_income, use_container_width=True)

            # Top ZIP codes by ethnicity concentration
            st.markdown("### 🏆 Top ZIP Codes by Demographic Characteristics")

            metric_choice = st.selectbox(
                "Select Metric",
                [
                    "Highest Population",
                    "Highest % High Income",
                    "Most Diverse",
                    "Highest White Population",
                    "Highest Black Population",
                    "Highest Hispanic Population",
                ],
                key="metric_choice_2",
            )

            if metric_choice == "Highest Population":
                top_zips = demo_filtered.nlargest(10, "total_pop")[
                    ["zip_code", "total_pop", "pct_high_income"]
                ]
            elif metric_choice == "Highest % High Income":
                top_zips = demo_filtered.nlargest(10, "pct_high_income")[
                    ["zip_code", "total_pop", "pct_high_income"]
                ]
            elif metric_choice == "Highest White Population":
                top_zips = demo_filtered.nlargest(10, "white_total")[
                    ["zip_code", "total_pop", "white_total"]
                ]
            elif metric_choice == "Highest Black Population":
                top_zips = demo_filtered.nlargest(10, "black_total")[
                    ["zip_code", "total_pop", "black_total"]
                ]
            elif metric_choice == "Highest Hispanic Population":
                top_zips = demo_filtered.nlargest(10, "hispanic_total")[
                    ["zip_code", "total_pop", "hispanic_total"]
                ]
            else:  # Most Diverse
                demo_filtered["diversity_score"] = 1 - (
                    (
                        demo_filtered["white_total"]
                        / demo_filtered["total_10_14"].replace(0, np.nan)
                    )
                    ** 2
                    + (
                        demo_filtered["black_total"]
                        / demo_filtered["total_10_14"].replace(0, np.nan)
                    )
                    ** 2
                    + (
                        demo_filtered["hispanic_total"]
                        / demo_filtered["total_10_14"].replace(0, np.nan)
                    )
                    ** 2
                ).fillna(0)
                top_zips = demo_filtered.nlargest(10, "diversity_score")[
                    ["zip_code", "total_pop", "diversity_score"]
                ]

            st.dataframe(top_zips, use_container_width=True)
    else:
        st.warning("Census data not available.")


# ==================== TAB 3: Geographic Maps ====================
with tab3:
    st.subheader("🗺️ Geographic Maps")

    if not school_df.empty and not location_df.empty:
        # Merge school and location data
        schools_with_location = school_df[
            school_df["latitude"].notna() & school_df["longitude"].notna()
        ].copy()

        col1, col2, col3 = st.columns(3)

        with col1:
            # State filter
            if "state" in schools_with_location.columns:
                states = sorted(
                    schools_with_location["state"].dropna().unique().tolist()
                )
                selected_states = st.multiselect(
                    "Select States",
                    options=states,
                    default=states[:5] if len(states) >= 5 else states,
                    key="state_filter_3",
                )
            else:
                selected_states = []

        with col2:
            # School type filter
            if "school_type" in schools_with_location.columns:
                school_types = (
                    schools_with_location["school_type"].dropna().unique().tolist()
                )
                selected_types = st.multiselect(
                    "School Type",
                    options=school_types,
                    default=school_types,
                    key="school_type_filter_3",
                )
            else:
                selected_types = []

        with col3:
            # Enrollment filter
            if "enrollment" in schools_with_location.columns:
                max_enrollment = (
                    int(schools_with_location["enrollment"].max())
                    if not schools_with_location["enrollment"].isna().all()
                    else 5000
                )
                enrollment_range = st.slider(
                    "Enrollment Range",
                    min_value=0,
                    max_value=min(max_enrollment, 5000),
                    value=(0, min(max_enrollment, 5000)),
                    key="enrollment_filter_3",
                )
            else:
                enrollment_range = (0, 5000)

        # Filter schools
        map_df = schools_with_location.copy()
        if selected_states:
            map_df = map_df[map_df["state"].isin(selected_states)]
        if selected_types:
            map_df = map_df[map_df["school_type"].isin(selected_types)]
        if "enrollment" in map_df.columns:
            map_df = map_df[
                (map_df["enrollment"] >= enrollment_range[0])
                & (map_df["enrollment"] <= enrollment_range[1])
            ]

        if not map_df.empty and len(map_df) > 0:
            # Limit to 10000 points for performance
            if len(map_df) > 10000:
                st.info(
                    f"Showing sample of 10,000 schools out of {len(map_df):,} total"
                )
                map_df = map_df.sample(n=10000, random_state=42)

            # Main map
            st.markdown("### 🏫 School Locations Map")

            map_color = st.selectbox(
                "Color by", ["School Type", "Enrollment", "State"], key="map_color_3"
            )

            color_col = {
                "School Type": "school_type",
                "Enrollment": "enrollment",
                "State": "state",
            }[map_color]

            fig_map = px.scatter_mapbox(
                map_df,
                lat="latitude",
                lon="longitude",
                color=color_col,
                size="enrollment" if "enrollment" in map_df.columns else None,
                hover_name="school_name",
                hover_data=["city", "state", "zip_code", "enrollment"],
                title=f"Schools by {map_color}",
                mapbox_style="carto-positron",
                zoom=3,
                height=600,
            )
            fig_map.update_layout(margin={"r": 0, "t": 50, "l": 0, "b": 0})
            st.plotly_chart(fig_map, use_container_width=True)

            # State-level summary
            st.markdown("### 📊 State-Level Summary")

            state_summary = (
                map_df.groupby("state")
                .agg(
                    {"school_name": "count", "enrollment": "sum", "teachers_fte": "sum"}
                )
                .reset_index()
            )
            state_summary.columns = [
                "State",
                "Number of Schools",
                "Total Enrollment",
                "Total Teachers",
            ]
            state_summary["Avg Students per School"] = (
                state_summary["Total Enrollment"] / state_summary["Number of Schools"]
            ).round(0)

            col_a, col_b = st.columns(2)

            with col_a:
                fig_states = px.bar(
                    state_summary.sort_values("Number of Schools", ascending=True).tail(
                        15
                    ),
                    y="State",
                    x="Number of Schools",
                    orientation="h",
                    title="Top 15 States by Number of Schools",
                    color="Number of Schools",
                    color_continuous_scale="Viridis",
                )
                fig_states.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig_states, use_container_width=True)

            with col_b:
                fig_enrollment = px.bar(
                    state_summary.sort_values("Total Enrollment", ascending=True).tail(
                        15
                    ),
                    y="State",
                    x="Total Enrollment",
                    orientation="h",
                    title="Top 15 States by Total Enrollment",
                    color="Total Enrollment",
                    color_continuous_scale="Plasma",
                )
                fig_enrollment.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig_enrollment, use_container_width=True)

            # ZIP code heatmap
            st.markdown("### 🔥 ZIP Code Concentration Heatmap")

            zip_counts = (
                map_df.groupby(["zip_code", "latitude", "longitude"])
                .size()
                .reset_index(name="school_count")
            )
            zip_counts = zip_counts[
                zip_counts["latitude"].notna() & zip_counts["longitude"].notna()
            ]

            if not zip_counts.empty:
                fig_heat = px.density_mapbox(
                    zip_counts,
                    lat="latitude",
                    lon="longitude",
                    z="school_count",
                    radius=20,
                    mapbox_style="carto-positron",
                    zoom=3,
                    title="School Density by Location",
                    height=500,
                )
                fig_heat.update_layout(margin={"r": 0, "t": 50, "l": 0, "b": 0})
                st.plotly_chart(fig_heat, use_container_width=True)
        else:
            st.warning(
                "No schools match the selected filters. Try adjusting your selections."
            )
    else:
        st.warning("School or location data not available.")


# ==================== TAB 4: Trend Analysis ====================
with tab4:
    st.subheader("📈 Trend Analysis Over Time")

    if not assessment_df.empty:
        # Year selection
        years = sorted(assessment_df["year"].dropna().unique())

        if len(years) > 1:
            col1, col2 = st.columns(2)

            with col1:
                race_for_trend = st.multiselect(
                    "Select Race/Ethnicity for Trend",
                    options=sorted(assessment_df["race"].dropna().unique()),
                    default=list(assessment_df["race"].dropna().unique())[:4],
                    key="race_trend_4",
                )

            with col2:
                score_metric = st.radio(
                    "Score Metric",
                    ["Math Proficiency", "Reading Proficiency"],
                    horizontal=True,
                    key="score_metric_4",
                )

            score_col = (
                "math_prof_mid"
                if score_metric == "Math Proficiency"
                else "read_prof_mid"
            )

            # Filter and aggregate by year and race
            trend_df = assessment_df[assessment_df["race"].isin(race_for_trend)]
            yearly_scores = (
                trend_df.groupby(["year", "race"])[score_col].mean().reset_index()
            )
            yearly_scores.columns = ["Year", "Race", "Proficiency"]

            if not yearly_scores.empty:
                fig_trend = px.line(
                    yearly_scores,
                    x="Year",
                    y="Proficiency",
                    color="Race",
                    markers=True,
                    title=f"{score_metric} Trends by Race/Ethnicity Over Time",
                    line_shape="spline",
                )
                fig_trend.update_layout(
                    xaxis_title="Year",
                    yaxis_title=f"{score_metric} (%)",
                    height=500,
                    hovermode="x unified",
                )
                st.plotly_chart(fig_trend, use_container_width=True)

                # Year-over-year change
                st.markdown("### 📊 Year-over-Year Change")

                pivot_scores = yearly_scores.pivot(
                    index="Year", columns="Race", values="Proficiency"
                )
                yoy_change = pivot_scores.diff()

                if not yoy_change.empty and len(yoy_change) > 1:
                    fig_yoy = px.bar(
                        yoy_change.reset_index().melt(
                            id_vars="Year", var_name="Race", value_name="Change"
                        ),
                        x="Year",
                        y="Change",
                        color="Race",
                        barmode="group",
                        title="Year-over-Year Change in Proficiency",
                    )
                    fig_yoy.update_layout(height=400)
                    st.plotly_chart(fig_yoy, use_container_width=True)
        else:
            st.info(
                "Multiple years of data required for trend analysis. Only one year available."
            )
    else:
        st.warning("Assessment data not available for trend analysis.")


# ==================== TAB 5: Custom Dashboard ====================
with tab5:
    st.subheader("🎯 Custom Dashboard Builder")

    st.markdown(
        """
    Build your own custom view by selecting the metrics and visualizations you want to see.
    """
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📊 Chart 1")
        chart1_type = st.selectbox(
            "Select Chart Type",
            [
                "Test Scores by Ethnicity",
                "Income Distribution",
                "School Enrollment",
                "Population by ZIP",
            ],
            key="chart1_type_5",
        )

        if chart1_type == "Test Scores by Ethnicity" and not assessment_df.empty:
            race_agg = (
                assessment_df.groupby("race")
                .agg({"math_prof_mid": "mean", "read_prof_mid": "mean"})
                .reset_index()
            )

            fig1 = px.bar(
                race_agg.melt(
                    id_vars="race", var_name="Subject", value_name="Proficiency"
                ),
                x="race",
                y="Proficiency",
                color="Subject",
                barmode="group",
                title="Average Proficiency by Race/Ethnicity",
            )
            st.plotly_chart(fig1, use_container_width=True)

        elif chart1_type == "Income Distribution" and not census_df.empty:
            census_df["high_income_pct"] = (
                census_df["hhi_220k_plus"] / census_df["total_pop"] * 100
            ).fillna(0)
            fig1 = px.histogram(
                census_df[census_df["high_income_pct"] > 0],
                x="high_income_pct",
                nbins=40,
                title="Distribution of High Income Households",
            )
            st.plotly_chart(fig1, use_container_width=True)

        elif chart1_type == "School Enrollment" and not school_df.empty:
            enrollment_data = school_df[school_df["enrollment"].notna()]
            fig1 = px.histogram(
                enrollment_data,
                x="enrollment",
                nbins=50,
                title="School Enrollment Distribution",
            )
            st.plotly_chart(fig1, use_container_width=True)

        elif chart1_type == "Population by ZIP" and not census_df.empty:
            top_pop = census_df.nlargest(20, "total_pop")
            fig1 = px.bar(
                top_pop,
                x="zip_code",
                y="total_pop",
                title="Top 20 ZIP Codes by Population",
            )
            st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.markdown("### 📊 Chart 2")
        chart2_type = st.selectbox(
            "Select Chart Type",
            [
                "Gender Distribution",
                "Ethnicity Breakdown",
                "School Types",
                "Income vs Population",
            ],
            key="chart2_type_5",
        )

        if chart2_type == "Gender Distribution" and not census_df.empty:
            gender_totals = pd.DataFrame(
                {
                    "Gender": ["Male", "Female"],
                    "Count": [
                        census_df["males_10_14"].sum(),
                        census_df["females_10_14"].sum(),
                    ],
                }
            )
            fig2 = px.pie(
                gender_totals,
                values="Count",
                names="Gender",
                title="Gender Distribution (Ages 10-14)",
            )
            st.plotly_chart(fig2, use_container_width=True)

        elif chart2_type == "Ethnicity Breakdown" and not census_df.empty:
            eth_totals = pd.DataFrame(
                {
                    "Ethnicity": ["White", "Black", "Hispanic"],
                    "Count": [
                        census_df["white_males_10_14"].sum()
                        + census_df["white_females_10_14"].sum(),
                        census_df["black_males_10_14"].sum()
                        + census_df["black_females_10_14"].sum(),
                        census_df["hispanic_males_10_14"].sum()
                        + census_df["hispanic_females_10_14"].sum(),
                    ],
                }
            )
            fig2 = px.pie(
                eth_totals,
                values="Count",
                names="Ethnicity",
                title="Ethnicity Breakdown (Ages 10-14)",
            )
            st.plotly_chart(fig2, use_container_width=True)

        elif chart2_type == "School Types" and not school_df.empty:
            school_type_counts = school_df["school_type"].value_counts().reset_index()
            school_type_counts.columns = ["Type", "Count"]
            fig2 = px.pie(
                school_type_counts,
                values="Count",
                names="Type",
                title="School Types Distribution",
            )
            st.plotly_chart(fig2, use_container_width=True)

        elif chart2_type == "Income vs Population" and not census_df.empty:
            census_sample = census_df.sample(min(1000, len(census_df)))
            census_sample["high_income"] = census_sample["hhi_220k_plus"].fillna(0)
            fig2 = px.scatter(
                census_sample,
                x="total_pop",
                y="high_income",
                title="High Income Households vs Total Population",
                trendline="ols",
            )
            st.plotly_chart(fig2, use_container_width=True)

    # Quick stats
    st.markdown("---")
    st.markdown("### 📈 Quick Statistics")

    metric_cols = st.columns(4)

    with metric_cols[0]:
        if not school_df.empty:
            st.metric("Total Schools", f"{len(school_df):,}")

    with metric_cols[1]:
        if not census_df.empty:
            st.metric("ZIP Codes", f"{census_df['zip_code'].nunique():,}")

    with metric_cols[2]:
        if not assessment_df.empty:
            avg_math = assessment_df["math_prof_mid"].mean()
            st.metric("Avg Math Proficiency", f"{avg_math:.1f}%")

    with metric_cols[3]:
        if not assessment_df.empty:
            avg_read = assessment_df["read_prof_mid"].mean()
            st.metric("Avg Reading Proficiency", f"{avg_read:.1f}%")


# Footer
st.markdown("---")
st.caption(
    "Interactive Analytics Dashboard • Census, Education, and School Data Analysis"
)
st.caption(
    "Use the filters to explore demographic patterns, test scores, and geographic distributions"
)
