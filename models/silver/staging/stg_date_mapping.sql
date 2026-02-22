{{ config(materialized='table') }}

select
  cast(initial_date as date) as old_dt,
  cast(new_date as date) as new_dt
from {{ source('bronze', 'date_mapping') }}