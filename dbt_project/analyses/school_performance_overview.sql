-- Analysis: School Performance Overview
-- This analysis provides a summary of school performance metrics

select
    state,
    county,
    count(*) as school_count,
    avg(math_test_pct_prof_midpt) as avg_math_proficiency,
    avg(read_test_pct_prof_midpt) as avg_reading_proficiency,
    avg(enrollment) as avg_enrollment
from {{ ref('dim_school_assessments') }}
where math_test_pct_prof_midpt is not null
  and read_test_pct_prof_midpt is not null
group by state, county
order by avg_math_proficiency desc, avg_reading_proficiency desc
