{{ config(materialized='table') }}

select
  cast(marketing_id as int64)          as marketing_id,
  cast(marketing_name as string)       as marketing_name,

  -- integer flag -> boolean
  case when cast(_is_from_ticketing as int64) = 1 then true else false end as is_from_ticketing,

  cast(_ticketing_customer_id as string) as ticketing_customer_id,

  cast(fn as string)                   as first_name,
  cast(ln as string)                   as last_name,
  cast(email as string)                as email,

  -- identity standardization (MVP)
  nullif(lower(trim(cast(email as string))),'')   as email_norm,
  cast(marketing_id as string)         as source_account_id,

  cast(_geo_bucket as string)          as geo_bucket,
  cast(city as string)                 as city,
  cast(state as string)                as state,

  -- zip should be string to preserve leading zeros
  cast(zip as string)                  as zip,

  cast(created_at as date)             as created_dt,

  -- integer flag -> boolean
  case when cast(is_email_opted_in as int64) = 1 then true else false end as is_email_opted_in,

  cast(source_system as string)        as source_system,
  cast(raw_file_name as string)        as raw_file_name

from {{ source('bronze', 'marketing_accounts') }}