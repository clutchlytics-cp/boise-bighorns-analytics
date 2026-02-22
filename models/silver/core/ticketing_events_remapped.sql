{{ config(materialized='table') }}

with e as (
  select *
  from {{ ref('stg_ticketing_events') }}
),

m as (
  select old_dt, new_dt
  from {{ ref('stg_date_mapping') }}
)

select
  e.*,

  -- remap event dates (only when mapping exists)
  coalesce(m_event.new_dt, e.event_dt)          as event_dt_remapped,
  coalesce(m_event_str.new_dt, e.event_dt_str)  as event_dt_str_remapped,

  -- remap on_sale_ts: keep time-of-day, shift only the date portion when mapped
  case
    when m_sale.new_dt is null then e.on_sale_ts
    else timestamp(
      datetime(
        m_sale.new_dt,
        time(e.on_sale_ts)
      )
    )
  end as on_sale_ts_remapped,

  -- remap season via simple case
  case
    when e.season = '2026-27' then '2024-25'
    else e.season
  end as season_remapped

from e
left join m as m_event
  on e.event_dt = m_event.old_dt
left join m as m_event_str
  on e.event_dt_str = m_event_str.old_dt
left join m as m_sale
  on date(e.on_sale_ts) = m_sale.old_dt