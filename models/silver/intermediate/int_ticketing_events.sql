{{ config(materialized='view') }}

with base as (

    select
        event_id_int as event_id,
        event_id as event_key,
        season,
        event_name,
        venue_name,
        event_dt,
        format_date('%A', cast(event_dt as date)) as day_of_week,
        event_start_time,
        case
            when is_promo_night then 1
            else 0
        end as is_promo_night,
        promo_name,
        opponent_team
    from {{ ref('stg_remapped_ticketing_events') }}
    where event_id_int is not null
      and event_dt is not null

),

deduped as (

    select
        * except(rn)
    from (
        select
            b.*,
            row_number() over (
                partition by event_key
                order by
                    (case when promo_name is not null then 1 else 0 end) desc,
                    (case when event_start_time is not null then 1 else 0 end) desc,
                    (case when venue_name is not null then 1 else 0 end) desc
            ) as rn
        from base b
    )
    where rn = 1

),

teams as (

    select
        team_id,
        team_name
    from {{ ref('dim_teams') }}

)

select
    d.event_key,
    d.event_id,
    d.season,
    d.event_name,
    d.venue_name,
    d.event_dt,
    d.day_of_week,
    d.event_start_time,
    d.is_promo_night,
    d.promo_name,

    -- team linkage
    t.team_id as opponent_team_id

from deduped d
left join teams t
  on d.opponent_team = t.team_name