{{ config(materialized='table') }}

with final AS (
    SELECT
    event_id,
    event_key,
    opponent_team_id,
    event_name,
    venue_name,
    event_dt,
    day_of_week,
    event_start_time,
    is_promo_night,
    promo_name
    FROM {{ref('int_ticketing_events')}}
)
SELECT * FROM final