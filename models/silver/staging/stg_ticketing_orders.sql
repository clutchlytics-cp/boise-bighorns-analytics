{{ config(materialized='table') }}

select
  -- keys / ids
  cast(order_id as string)                as order_id,
  cast(order_seq as int64)                as order_seq,
  cast(event_id as string)                as event_id,
  cast(customer_id as string)             as customer_id,
  cast(order_event_key as string)         as order_event_key,

  -- descriptive
  cast(season as string)                  as season,
  cast(channel as string)                 as channel,
  cast(email as string)                   as email,
  cast(payment_method as string)          as payment_method,
  cast(order_status as string)            as order_status,

  -- dates
  cast(order_datetime as timestamp)       as order_ts,
  date(cast(order_datetime as timestamp)) as order_dt,

  -- quantities / amounts (normalize types)
  cast(ticket_qty as int64)               as ticket_qty,
  cast(avg_ticket_price_est as numeric)   as avg_ticket_price_est,
  cast(subtotal_amount as numeric)        as subtotal_amount,
  cast(fees_amount as numeric)            as fees_amount,
  cast(tax_amount as numeric)             as tax_amount,
  cast(total_amount as numeric)           as total_amount,

  -- lineage
  cast(file_name_raw as string)           as file_name_raw

from {{ source('bronze', 'ticketing_orders') }}