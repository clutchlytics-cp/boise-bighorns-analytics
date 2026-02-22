{{ config(materialized='table') }}

select
  cast(product_id as int64)           as product_id,
  cast(category as string)            as category,
  cast(product_name as string)        as product_name,
  cast(product_variant as string)     as product_variant,
  cast(sku as string)                 as sku,

  cast(base_price as numeric)         as base_price,
  cast(unit_cost as numeric)          as unit_cost,

  -- is_alcohol came in as INTEGER; convert to boolean safely
  case
    when cast(is_alcohol as int64) = 1 then true
    else false
  end as is_alcohol,

  cast(source_system as string)       as source_system,
  cast(raw_file_name as string)       as raw_file_name

from {{ source('bronze', 'pos_products') }}