{{ config(materialized='table') }}

-- Golden table intended grain: one row per (ncessch) for academic year 2021
-- Duplicate sources previously:
--  * location_data grouped by an unselected 'county' column -> implicit duplication
--  * grade_eight included multiple grades per school -> row explosion when joining on ncessch
--  * possible duplicate directory rows per school
-- Strategy: normalize each CTE to its key, then join; add ROW_NUMBER diagnostics.

WITH location_data AS (
        SELECT
                zip,
                county_fips,
                state,
                state_fips,
                AVG(latitude) AS avg_lat,
                AVG(longitude) AS avg_lon,
                COUNT(*) AS coordinate_count
        FROM test.location_data
        WHERE latitude IS NOT NULL
            AND longitude IS NOT NULL
        GROUP BY zip, county_fips, state, state_fips  -- removed stray 'county'
),
census_2021 AS (
    SELECT
        c.zip_code,
        c.total_pop,
        c.hhi_150k_200k,
        c.hhi_220k_plus,
        loc.state,
        loc.state_fips,
        loc.county_fips,
        loc.avg_lat,
        loc.avg_lon,
        loc.coordinate_count
    FROM test.census_data c
    LEFT JOIN location_data loc ON c.zip_code = loc.zip
    WHERE c.year = 2021
      AND c.total_pop > 0  -- Filter out empty areas
),
grade_eight AS (
    -- Only keep grade 8; collapse to single row per school
    SELECT
        ncessch,
        SUM(enrollment::numeric) as grade_eight_enrollment
    FROM test.urban_ccd_enrollment_grade_8_exp
    WHERE (year_json::numeric)=2021
      AND grade = '8'
    GROUP BY ncessch
),
schools_2021 AS (
    SELECT DISTINCT
        d.school_name,
        d.ncessch,
        d.school_status,
        d.enrollment,
		ge.grade_eight_enrollment,
        d.school_type,
        d.zip_location AS zip_code
    FROM test.urban_ccd_directory_exp d
	LEFT JOIN grade_eight ge ON ge.ncessch = d.ncessch
    WHERE (d.year_json::numeric)=2021
),
tests_2021 AS (
	SELECT
		ncessch,
		max(math_test_num_valid) as math_test_num_valid ,
		max(math_test_pct_prof_high) as math_test_pct_prof_high,
		max(math_test_pct_prof_low) as math_test_pct_prof_low,
		max(math_test_pct_prof_midpt) as math_test_pct_prof_midpt,
		max(read_test_num_valid) as read_test_num_valid,
		max(read_test_pct_prof_high) as read_test_pct_prof_high,
		max(read_test_pct_prof_low) as read_test_pct_prof_low,
		max(read_test_pct_prof_midpt) as read_test_pct_prof_midpt
	FROM test.urban_edfacts_assessments_grade_8_race_sex_exp
	WHERE (year_json::numeric)=2020
	GROUP BY ncessch
),
walk as(
    SELECT
        statefp,
        countyfp,
        AVG(natwalkind) as avg_natwalkind
    FROM test.walk_index
    GROUP BY statefp, countyfp
),
final_dataset AS (
    SELECT
		s.school_name,
        --s.ncessch,
        s.school_type,
        s.enrollment,
		s.grade_eight_enrollment,
		test.math_test_num_valid::numeric as math_counts,
		test.math_test_pct_prof_high::numeric as math_high_pct,
		test.math_test_pct_prof_low::numeric as math_low_pct,
		test.read_test_num_valid::numeric as read_counts,
		test.read_test_pct_prof_high::numeric as read_high_pct,
		test.read_test_pct_prof_low::numeric as read_low_pct,
        --c.zip_code,
        --c.state,
        ROUND(c.hhi_150k_200k::DECIMAL / NULLIF(c.total_pop, 0) * 100, 2) AS pct_hhi_150k_200k,
        ROUND(c.hhi_220k_plus::DECIMAL / NULLIF(c.total_pop, 0) * 100, 2) AS pct_hhi_220k_plus,
        walk.avg_natwalkind,
        c.total_pop,
        c.hhi_150k_200k,
        c.hhi_220k_plus,
        COUNT(*) OVER (PARTITION BY c.zip_code, c.state) AS schools_in_zip,
        ROW_NUMBER() OVER (PARTITION BY s.ncessch ORDER BY c.state, c.zip_code) AS dup_rank
    FROM census_2021 c
    INNER JOIN schools_2021 s ON c.zip_code = s.zip_code
	INNER JOIN tests_2021 test ON test.ncessch = s.ncessch
    INNER JOIN walk ON walk.statefp = c.state_fips AND walk.countyfp = c.county_fips
)
SELECT *
FROM final_dataset
WHERE dup_rank = 1  -- enforce one row per school
ORDER BY school_name
