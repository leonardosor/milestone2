# Milestone 2 Analytics - dbt Project

This dbt project transforms raw census and education data into analytics-ready datasets.

## Project Structure

```
dbt_project/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Database connection profiles
├── models/
│   ├── sources.yml          # Source table definitions
│   ├── staging/             # Staging models (cleaned raw data)
│   │   ├── stg_census_data.sql
│   │   ├── stg_school_directory.sql
│   │   ├── stg_school_assessments.sql
│   │   ├── stg_location_data.sql
│   │   └── schema.yml
│   └── marts/               # Business logic models
│       ├── dim_school_assessments.sql
│       └── schema.yml
├── macros/                  # Reusable SQL functions
│   └── safe_percentage.sql
├── tests/                   # Data quality tests
│   └── test_math_proficiency_range.sql
├── analyses/                # Analytical queries
│   └── school_performance_overview.sql
├── seeds/                   # Static reference data
├── snapshots/               # Slowly changing dimensions
└── requirements_dbt.txt     # Python dependencies
```

## Setup

1. Install dbt and dependencies:
   ```bash
   pip install -r requirements_dbt.txt
   ```

2. Ensure your `.env` file contains the required database credentials:
   ```
   LOCAL_HOST=your_host
   LOCAL_USER=your_username
   LOCAL_PW=your_password
   LOCAL_DB=your_database
   LOCAL_PORT=5432
   ```

3. Test the connection:
   ```bash
   cd dbt_project
   dbt debug
   ```

## Running the Project

1. Install dbt packages (if any):
   ```bash
   dbt deps
   ```

2. Run all models:
   ```bash
   dbt run
   ```

3. Run tests:
   ```bash
   dbt test
   ```

4. Generate documentation:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## Model Descriptions

### Staging Models
- **stg_census_data**: Cleaned census demographic data
- **stg_school_directory**: School location and basic information
- **stg_school_assessments**: Student assessment results
- **stg_location_data**: Geographic coordinate reference data

### Mart Models
- **dim_school_assessments**: Comprehensive school performance dimension combining directory and assessment data

## Data Sources

The project reads from the following raw tables:
- `census_data`: Census demographic information
- `urban_data_directory`: School directory with location data
- `urban_data_expanded`: School assessment results
- `location_data`: Geographic reference data
