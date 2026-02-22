{{ config(materialized='table') }}

select
  -- keys (keep both; we'll decide later which is canonical)
  cast(event_id as int64)            as event_id_int,
  cast(event_id_num as string)       as event_id,

  -- descriptive
  cast(season as string)             as season,
  cast(opponent_team as string)      as opponent_team,
  cast(event_name as string)         as event_name,
  cast(venue_name as string)         as venue_name,
  cast(day_of_week as string)        as day_of_week,

  -- dates / times
  cast(event_date as date)           as event_dt,
  cast(event_date_str as date)       as event_dt_str,
  cast(event_start_time as string)   as event_start_time,
  cast(on_sale_datetime as timestamp) as on_sale_ts,

  -- promo
  cast(is_promo_night as bool)       as is_promo_night,
  cast(promo_name as string)         as promo_name,

  -- capacity
  cast(capacity_reported as int64)   as capacity_reported,

  -- lineage
  cast(source_system as string)      as source_system,
  cast(raw_file_name as string)      as raw_file_name

from {{ source('bronze', 'ticketing_events') }}