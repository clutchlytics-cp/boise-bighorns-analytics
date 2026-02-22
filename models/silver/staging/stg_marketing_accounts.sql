{{ config(materialized='table') }}

with src as (
  select
    cast(marketing_id as int64)          as marketing_id,
    cast(marketing_name as string)       as marketing_name,

    case when cast(_is_from_ticketing as int64) = 1 then true else false end as is_from_ticketing,
    cast(_ticketing_customer_id as string) as ticketing_customer_id,

    cast(fn as string)                   as first_name,
    cast(ln as string)                   as last_name,
    nullif(lower(trim(cast(email as string))),'')   as email_norm,

    cast(_geo_bucket as string)          as geo_bucket,
    cast(city as string)                 as city,
    cast(state as string)                as state,
    cast(zip as string)                  as zip,

    cast(created_at as date)             as created_dt_old,

    case when cast(is_email_opted_in as int64) = 1 then true else false end as is_email_opted_in,

    cast(source_system as string)        as source_system,
    cast(raw_file_name as string)        as raw_file_name
  from {{ source('bronze', 'marketing_accounts') }}
),

final as (
  select
    s.marketing_id,
    s.marketing_name,
    s.is_from_ticketing,
    s.ticketing_customer_id,

    s.first_name,
    s.last_name,
    s.email_norm,

    s.geo_bucket,
    s.city,
    s.state,
    s.zip,

    coalesce(dm.new_dt, s.created_dt_old) as created_dt,

    s.is_email_opted_in,
    s.source_system,
    s.raw_file_name
  from src s
  left join {{ ref('stg_date_mapping') }} dm
    on dm.old_dt = s.created_dt_old
)

select * from final