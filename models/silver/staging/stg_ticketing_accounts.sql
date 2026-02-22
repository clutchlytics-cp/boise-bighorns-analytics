{{ config(materialized='table') }}

with src as (
  select
    cast(customer_num as int64)     as customer_num,
    cast(customer_id as string)     as customer_id,

    cast(created_at as date)        as created_dt_old,

    cast(first_name as string)      as first_name,
    cast(last_name as string)       as last_name,
    cast(email as string)           as email,

    cast(city as string)            as city,
    cast(state as string)           as state,
    cast(zip as string)             as zip,

    cast(source_system as string)   as source_system,
    cast(raw_file_name as string)   as raw_file_name
  from {{ source('bronze', 'ticketing_accounts') }}
),

final as (
  select
    s.customer_num,
    s.customer_id,

    coalesce(dm.new_dt, s.created_dt_old) as created_dt,

    s.first_name,
    s.last_name,
    s.email,
    s.city,
    s.state,
    s.zip,
    s.source_system,
    s.raw_file_name
  from src s
  left join {{ ref('stg_date_mapping') }} dm
    on dm.old_dt = s.created_dt_old
)

select * from final