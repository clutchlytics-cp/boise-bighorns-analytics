{{ config(materialized='view') }}

with marketing as (

  select
    'marketing' as source_system,
    cast(marketing_id as string) as source_account_id,
    created_dt,
    email_norm,
    city,
    state,
    zip
  from {{ ref('stg_marketing_accounts') }}
  where email_norm is not null

),

ticketing as (

  select
    'ticketing' as source_system,
    cast(customer_num as string) as source_account_id,
    created_dt,
    email_norm,
    city,
    state,
    zip
  from {{ ref('stg_ticketing_accounts') }}
  where email_norm is not null

)

select * from marketing
union all
select * from ticketing