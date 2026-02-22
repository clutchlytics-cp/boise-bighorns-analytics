{{ config(materialized='table') }}

select 'lower' as seat_inventory_group, * from {{ ref('stg_seat_manifest_lower') }}
union all
select 'upper' as seat_inventory_group, * from {{ ref('stg_seat_manifest_upper') }}
union all
select 'club'  as seat_inventory_group, * from {{ ref('stg_seat_manifest_club') }}
union all
select 'ada'   as seat_inventory_group, * from {{ ref('stg_seat_manifest_ada') }}