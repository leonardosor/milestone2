{{ config(materialized='view') }}

select
    cast(latitude as decimal(10,6)) as latitude,
    cast(longitude as decimal(10,6)) as longitude,
    current_timestamp as loaded_at
from {{ source('raw_data', 'location_data') }}
