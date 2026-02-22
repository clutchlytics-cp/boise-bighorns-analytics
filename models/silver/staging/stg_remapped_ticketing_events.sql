{{ config(materialized='table') }}


with selectedseason as (
SELECT *
FROM {{ref('stg_ticketing_events')}}
WHERE season <> '2024-25'
),
final as(
SELECT 

s.event_id_int,
s.event_id,
CASE 
    WHEN s.season = '2026-27' THEN  '2024-2025'
    WHEN s.season = '2025-26' THEN '2025-2026'
    ELSE s.season
END AS season,
s.opponent_team,
s.event_name,
s.venue_name,
s.day_of_week,
coalesce(dm.new_dt, s.event_dt) as event_dt,
s.event_start_time,
s.is_promo_night,
s.promo_name
from selectedseason s
LEFT JOIN {{ref('stg_date_mapping')}} dm
ON dm.old_dt = event_dt
)

SELECT * FROM final