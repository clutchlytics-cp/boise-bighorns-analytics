{{ config(materialized='table') }}

with base as (
  select
    *
  from {{ ref('team_mapping') }}
)
SELECT * FROM base