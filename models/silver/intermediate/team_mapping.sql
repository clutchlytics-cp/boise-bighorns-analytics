{{ config(
    materialized='incremental',
    unique_key='team_name',
    incremental_strategy='merge'
) }}

-- 1️⃣ All distinct teams from int layer
with source_teams as (

    select distinct
        regexp_replace(trim(opponent_team), r'\s+', ' ') as team_name
    from {{ ref('stg_remapped_ticketing_events') }}
    where opponent_team is not null
      and trim(opponent_team) != ''

),

existing as (

    {% if is_incremental() %}
        select team_id, team_name
        from {{ this }}
    {% else %}
        -- BigQuery-safe empty relation (no WHERE without FROM)
        select
            cast(null as int64) as team_id,
            cast(null as string) as team_name
        limit 0
    {% endif %}

),

new_teams as (

    select s.team_name
    from source_teams s
    left join existing e
        on s.team_name = e.team_name
    where e.team_name is null

),

-- 4️⃣ Find current max team_id
max_id as (

    {% if is_incremental() %}
        select coalesce(max(team_id), 0) as max_team_id
        from {{ this }}
    {% else %}
        select 0 as max_team_id
    {% endif %}

)

select
    m.max_team_id + row_number() over (order by n.team_name) as team_id,
    n.team_name
from new_teams n
cross join max_id m