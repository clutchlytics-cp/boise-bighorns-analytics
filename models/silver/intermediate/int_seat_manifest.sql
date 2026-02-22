-- models/silver/intermediate/int_seat_manifest.sql
-- Purpose:
--   Normalize seat manifest records into a single, deduped, analytics-ready “silver” model.
--   No SCD logic (current-state only).

{{ config(materialized='table') }}

with src as (

  select
    seat_inventory_group,
    seat_seq,
    section,
    row_label,
    seat_number,
    seat_key,
    seat_type,
    zone,
    row_tier,
    base_price,
    source_system,
    raw_file_name
  from {{ ref('stg_seat_manifest_all') }}

),

cleaned as (

  select

    -- Standardized / trimmed / cased
    upper(trim(seat_inventory_group)) as seat_inventory_group,
    safe_cast(seat_seq as int64)      as seat_seq,

    upper(trim(section))              as section,
    upper(trim(row_label))            as row_label,
    safe_cast(seat_number as int64)   as seat_number,
    upper(trim(seat_key))             as seat_key,

    upper(trim(seat_type))            as seat_type,
    upper(trim(zone))                 as zone,
    upper(trim(row_tier))             as row_tier,

    safe_cast(base_price as numeric)  as base_price,

    upper(trim(source_system))        as source_system,
    raw_file_name

  from src

),

normalized as (

  select
    -- Normalize zone to an accepted, stable set
    case
      when zone in ('LOWER', 'LOWERBOWL', 'LOWER_BOWL') then 'LOWER'
      when zone in ('UPPER', 'UPPERBOWL', 'UPPER_BOWL') then 'UPPER'
      when zone in ('CLUB', 'CLUBLEVEL', 'CLUB_LEVEL')  then 'CLUB'
      when zone in ('SUITE', 'SUITES')                  then 'SUITE'
      when zone in ('FLOOR', 'COURTSIDE')               then 'FLOOR'
      else zone
    end as zone,

    -- Normalize row_tier to FRONT / MID / BACK
    case
      when row_tier in ('FRONT', 'FRT', '1') then 'FRONT'
      when row_tier in ('MID', 'MIDDLE', '2') then 'MID'
      when row_tier in ('BACK', 'REAR', '3') then 'BACK'
      else row_tier
    end as row_tier,

    -- Normalize seat_type to a clean set
    case
      when seat_type in ('REG', 'REGULAR', 'STANDARD') then 'REGULAR'
      when seat_type in ('CLUB') then 'CLUB'
      when seat_type in ('ADA', 'ACCESSIBLE') then 'ADA'
      when seat_type in ('SUITE') then 'SUITE'
      else seat_type
    end as seat_type,

    -- Normalize inventory group
    case
      when seat_inventory_group in ('LOWER') then 'LOWER'
      when seat_inventory_group in ('UPPER') then 'UPPER'
      when seat_inventory_group in ('CLUB')  then 'CLUB'
      when seat_inventory_group in ('ADA')   then 'ADA'
      when seat_inventory_group in ('SUITE') then 'SUITE'
      else seat_inventory_group
    end as seat_inventory_group,

    -- Pass-through (already cleaned)
    seat_seq,
    section,
    row_label,
    seat_number,
    base_price,
    source_system,
    seat_key,
    raw_file_name

  from cleaned

),

keys as (

  select
    *,

    -- Helpful flags for downstream QA / bucketing
    case
      when seat_type = 'ADA'
        or seat_inventory_group = 'ADA'
        or starts_with(section, 'ADA')
      then true else false
    end as is_ada,

    case when zone = 'CLUB' or seat_inventory_group = 'CLUB' or seat_type = 'CLUB'
      then true else false
    end as is_club,

    case when zone = 'SUITE' or seat_inventory_group = 'SUITE' or seat_type = 'SUITE'
      then true else false
    end as is_suite,

    -- QA signal: does supplied seat_key match our canonical key (when supplied)?
    case
      when seat_key is null or trim(seat_key) = '' then false
      when upper(trim(seat_key)) = concat(section, '-', row_label, '-', cast(seat_number as string)) then false
      else true
    end as seat_key_mismatch_flag

  from normalized

),

deduped as (

  select *
  from (
    select
      *,
      row_number() over (
        partition by seat_key
        order by
          -- Prefer ticketing as primary if multiple sources ever show up
          case when source_system = 'TICKETING' then 1 else 2 end,
          -- Prefer records that have a seat_seq
          case when seat_seq is not null then 1 else 2 end,
          -- Deterministic tie-breakers
          raw_file_name desc,
          seat_seq desc
      ) as _rn
    from keys
    where seat_key is not null
  )
  where _rn = 1

)

select
  -- Core canonical fields
  seat_key,
  seat_seq,
  section,
  row_label,
  seat_number,

  -- Normalized classification
  seat_inventory_group,
  seat_type,
  zone,
  row_tier,

  -- Pricing
  base_price,

  -- Convenience flags
  is_ada,
  is_club,
  is_suite,

  -- Lineage
  source_system,
  raw_file_name,

  -- QA flags
  seat_key_mismatch_flag

from deduped
