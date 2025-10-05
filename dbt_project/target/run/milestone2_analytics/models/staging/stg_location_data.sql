
  create view "milestone2"."public"."stg_location_data__dbt_tmp"
    
    
  as (
    

select
    cast(latitude as decimal(10,6)) as latitude,
    cast(longitude as decimal(10,6)) as longitude,
    current_timestamp as loaded_at
from "milestone2"."public"."location_data"
  );