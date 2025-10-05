{{ config(materialized='view') }}

select
    zip_code, 
    total_pop,
    hhi_150k_200k,
    hhi_200k_plus,
    males_15_17,
    females_15_17,
    white_males_15_17,
    black_males_15_17,
    hispanic_males_15_17,
    white_males_15_17,
    black_females_15_17,
    hispanic_females_15_17
FROM public.census_data cd
LEFT JOIN marts.location_data

