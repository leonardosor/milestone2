

select
    lea_name,
    leaid,
    ncessch,
    year,
    econ_disadvantaged,
    enrollment,
    grade,
    math_test_num_valid,
    math_test_pct_prof_high,
    math_test_pct_prof_low,
    math_test_pct_prof_midpt,
    migrant,
    race,
    read_test_num_valid,
    read_test_pct_prof_high,
    read_test_pct_prof_low,
    read_test_pct_prof_midpt,
    sex,
    current_timestamp as loaded_at
from "milestone2"."public"."urban_data_expanded"