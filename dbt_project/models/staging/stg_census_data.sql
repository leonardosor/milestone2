{{ config(materialized='view') }}

select
    state,
    county,
    zip,
    -- Add other relevant census columns here based on your actual schema
    current_timestamp as loaded_at
from {{ source('raw_data', 'census_data') }}
