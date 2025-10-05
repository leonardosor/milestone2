{{ config(materialized='view') }}

select
    leaid,
    ncessch,
    school_name,
    school_status,
    school_type,
    cast(latitude as decimal(10,6)) as latitude,
    cast(longitude as decimal(10,6)) as longitude,
    city_mailing,
    county,
    state,
    zip_mailing,
    enrollment,
    year,
    current_timestamp as loaded_at
from {{ source('raw_data', 'urban_data_directory') }}
where latitude is not null
  and longitude is not null
  and latitude != ''
  and longitude != ''
