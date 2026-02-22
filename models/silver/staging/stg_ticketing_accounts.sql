{{ config(materialized='table') }}

select
  cast(customer_num as int64)     as customer_num,
  cast(customer_id as string)     as customer_id,

  cast(created_at as date)        as created_dt,

  cast(first_name as string)      as first_name,
  cast(last_name as string)       as last_name,
  cast(email as string)           as email,

  -- identity standardization (MVP)
  nullif(lower(trim(cast(email as string))),'') as email_norm,
  cast(customer_num as string)       as source_account_id,

  cast(city as string)            as city,
  cast(state as string)           as state,
  cast(zip as string)             as zip,   -- store zip as string to preserve leading zeros

  cast(source_system as string)   as source_system,
  cast(raw_file_name as string)   as raw_file_name

from {{ source('bronze', 'ticketing_accounts') }}