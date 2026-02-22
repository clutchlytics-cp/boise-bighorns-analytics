{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['source_system', 'source_account_id']
  )
}}

with identity_candidates as (

  select
    source_system,
    source_account_id,
    email_norm,
    first_name,
    last_name,
    city,
    state,
    zip,
    created_dt
  from {{ ref('int_identity_candidates') }}
  where email_norm is not null

),

existing_email_map as (

  {% if is_incremental() %}
    select
      email_norm,
      min(bighorn_id) as bighorn_id
    from {{ this }}
    group by 1
  {% else %}
    -- first run: return an empty set without referencing {{ this }}
    select
      x.email_norm,
      x.bighorn_id
    from (
      select
        cast(null as string) as email_norm,
        cast(null as int64) as bighorn_id
    ) x
    where false
  {% endif %}

),

max_existing as (

  {% if is_incremental() %}
    select coalesce(max(bighorn_id), 0) as max_bighorn_id
    from {{ this }}
  {% else %}
    select 0 as max_bighorn_id
  {% endif %}

),

new_emails as (

  select distinct c.email_norm
  from identity_candidates c
  left join existing_email_map e
    on e.email_norm = c.email_norm
  where e.email_norm is null

),

assigned_new_email_ids as (

  select
    email_norm,
    (select max_bighorn_id from max_existing)
      + row_number() over (order by email_norm) as bighorn_id
  from new_emails

),

resolved as (

  select
    c.source_system,
    c.source_account_id,
    c.email_norm,
    coalesce(e.bighorn_id, n.bighorn_id) as bighorn_id,

    min(c.created_dt) over (partition by c.source_system, c.source_account_id) as first_seen_dt,
    max(c.created_dt) over (partition by c.source_system, c.source_account_id) as last_seen_dt,

    c.first_name,
    c.last_name,
    c.city,
    c.state,
    c.zip
  from identity_candidates c
  left join existing_email_map e
    on e.email_norm = c.email_norm
  left join assigned_new_email_ids n
    on n.email_norm = c.email_norm

)

select * from resolved