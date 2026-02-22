{{ config(materialized='table') }}

select
  -- keys
  cast(order_id as string)        as order_id,
  cast(event_id as string)        as event_id,
  cast(seat_key as string)        as seat_key,

  -- status / attributes
  cast(order_status as string)    as order_status,
  cast(channel as string)         as channel,
  cast(seat_source as string)     as seat_source,
  cast(ticket_type as string)     as ticket_type,
  cast(barcode as string)         as barcode,

  -- measures
  cast(quantity as int64)         as quantity,
  cast(price_each as numeric)     as price_each,
  cast(line_total as numeric)     as line_total,

  -- lineage
  cast(raw_file_name as string)   as raw_file_name

from {{ source('bronze', 'ticketing_order_items') }}