-- Test to ensure math proficiency percentages are within valid range (0-100)
select *
from {{ ref('dim_school_assessments') }}
where math_test_pct_prof_midpt < 0
   or math_test_pct_prof_midpt > 100
