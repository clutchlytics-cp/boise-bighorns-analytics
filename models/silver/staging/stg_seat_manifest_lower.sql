{{ config(materialized='table') }}

select
  cast(seat_seq as int64)        as seat_seq,
  cast(section as string)        as section,
  cast(row as string)            as row_label,
  cast(seat_number as int64)     as seat_number,
  cast(seat_key as string)       as seat_key,
  cast(seat_type as string)      as seat_type,
  cast(zone as string)           as zone,
  cast(row_tier as string)       as row_tier,
  cast(base_price as numeric)    as base_price,

  cast(source_system as string)  as source_system,
  cast(raw_file_name as string)  as raw_file_name
from {{ source('bronze', 'seat_manifest_lower') }}