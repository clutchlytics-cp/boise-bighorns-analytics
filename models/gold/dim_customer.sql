{{ config(materialized='table') }}

with base as (
  select
    bighorn_id,
    email_norm,
    first_name,
    last_name,
    city,
    state,
    zip,
    source_system,
    source_account_id,
    first_seen_dt,
    last_seen_dt
  from {{ ref('customer_mapping_identity') }}
),

roll as (
  select
    bighorn_id,

    -- stable identifiers
    min(email_norm) as email_norm,

    -- lifecycle
    min(first_seen_dt) as first_seen_dt,
    max(last_seen_dt) as last_seen_dt,

    -- customer footprint / metadata
    count(*) as source_account_rows,
    count(distinct source_system) as source_system_count,
    count(distinct source_account_id) as source_account_count,

    max(case when source_system = 'ticketing' then 1 else 0 end) as has_ticketing_account,
    max(case when source_system = 'marketing' then 1 else 0 end) as has_marketing_account,

    -- canonical attributes: choose most recently seen non-null value
    array_agg(first_name ignore nulls order by last_seen_dt desc limit 1)[offset(0)] as first_name,
    array_agg(last_name  ignore nulls order by last_seen_dt desc limit 1)[offset(0)] as last_name,
    array_agg(city       ignore nulls order by last_seen_dt desc limit 1)[offset(0)] as city,
    array_agg(state      ignore nulls order by last_seen_dt desc limit 1)[offset(0)] as state,
    array_agg(zip        ignore nulls order by last_seen_dt desc limit 1)[offset(0)] as zip

  from base
  group by 1
)

select
  bighorn_id,
  email_norm,
  first_name,
  last_name,
  city,
  state,
  zip,
  first_seen_dt,
  last_seen_dt,
  has_ticketing_account,
  has_marketing_account,
  source_system_count,
  source_account_count,
  source_account_rows
from roll