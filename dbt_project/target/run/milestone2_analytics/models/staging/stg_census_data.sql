
  create view "milestone2"."public"."stg_census_data__dbt_tmp"
    
    
  as (
    

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
LEFT JOIN marts.location_data

from "milestone2"."public"."census_data"
  );