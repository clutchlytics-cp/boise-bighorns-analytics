{{ config(materialized='table') }}

select
  -- keys
  cast(transaction_id as int64)        as transaction_id,
  cast(line_id as int64)               as line_id,
  cast(event_id as string)             as event_id,
  cast(product_id as int64)            as product_id,

  -- dates (your transaction_datetime is a DATE, not a TIMESTAMP)
  cast(_event_start_dt as date)        as event_start_dt,
  cast(transaction_datetime as date)   as transaction_dt,

  -- identifiers / context
  cast(_barcode as string)             as barcode,
  cast(stand_location as string)       as stand_location,

  -- optional fields coming in with underscores (keep clean names in silver)
  cast(_category as string)            as category,

  -- measures
  cast(_unit_price as numeric)         as unit_price,
  cast(quantity as int64)              as quantity,
  cast(gross_sale as numeric)          as gross_sale,

  -- payment / channel
  cast(payment_type as string)         as payment_type,
  cast(transaction_channel as string)  as transaction_channel,

  -- lineage
  cast(source_system as string)        as source_system,
  cast(raw_file_name as string)        as raw_file_name

from {{ source('bronze', 'pos_transactions') }}