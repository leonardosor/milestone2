{{ config(materialized='table') }}

with school_location as (
    select
        sd.*,
        ld.latitude as location_latitude,
        ld.longitude as location_longitude
    from {{ ref('stg_school_directory') }} sd
    left join {{ ref('stg_location_data') }} ld
        on sd.latitude = ld.latitude
        and sd.longitude = ld.longitude
),

school_facts as (
    select
        city_mailing,
        county,
        state,
        zip_mailing,
        leaid,
        ncessch,
        school_name,
        school_status,
        school_type,
        enrollment,
        year
    from school_location
),

final as (
    select
        sf.*,
        sa.lea_name,
        sa.econ_disadvantaged,
        sa.grade,
        sa.math_test_num_valid,
        sa.math_test_pct_prof_high,
        sa.math_test_pct_prof_low,
        sa.math_test_pct_prof_midpt,
        sa.migrant,
        sa.race,
        sa.read_test_num_valid,
        sa.read_test_pct_prof_high,
        sa.read_test_pct_prof_low,
        sa.read_test_pct_prof_midpt,
        sa.sex
    from school_facts sf
    right join {{ ref('stg_school_assessments') }} sa
        on sf.leaid = sa.leaid
        and sf.ncessch = sa.ncessch
        and sf.year = sa.year
)

select * from final
