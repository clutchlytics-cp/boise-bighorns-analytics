-- models/gold/dimensions/dim_seat_manifest.sql
-- Purpose:
--   Gold dimension for seat inventory (one row per physical seat).
--   Current-state only (no SCD). Built from silver int_seat_manifest.

{{ config(materialized='table') }}

with seats as (

  select
    seat_key,
    seat_seq,
    section,
    row_label,
    seat_number,
    seat_inventory_group,
    seat_type,
    zone AS seat_zone,
    row_tier,
    base_price,
    is_ada,
    is_club,
    is_suite
  from {{ ref('int_seat_manifest') }}

),

final as (

  select
    -- Surrogate key (stable join key for facts)
    {{ dbt_utils.generate_surrogate_key(['seat_key']) }} as seat_id,

    -- Natural / physical key
    seat_key,

    -- Physical location
    section,
    row_label,
    seat_number,

    -- Convenience label for reporting
    concat(section, ' Row ', row_label, ' Seat ', cast(seat_number as string)) as full_seat_label,

    -- Classification
    seat_inventory_group,
    seat_type,
    seat_zone,
    row_tier,

    -- Business bucketing
    case
      when is_suite = 1 then 'SUITE'
      when is_club = 1 then 'PREMIUM'
      when is_ada = 1 then 'ADA'
      else 'GENERAL'
    end as inventory_bucket,

    -- Business flags (current-state)
    is_ada,
    is_club,
    is_suite,

    case
      when is_suite = 1 then 1
      when is_club =1 then 1
      else 0
    end as is_premium_seat,

    -- “Policy” flag (adjust later if you decide ADA should be eligible)
    case
      when is_suite = 0 then 0
      else 1
    end as is_discount_eligible,

    -- Pricing
    base_price,

    -- Optional: simple tiering off price (kept intentionally generic)
    case
      when base_price is null then 'UNKNOWN'
      when base_price < 60 then 'LOW'
      when base_price < 120 then 'MID'
      else 'HIGH'
    end as price_tier,

    -- Operational / lineage
    seat_seq

  from seats

)

select * from final