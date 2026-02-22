{{ config(materialized='table') }}

select
  cast(session_id as string)                 as session_id,
  cast(session_date as date)                 as session_dt,

  cast(customer_id as int64)                 as marketing_customer_id,

  -- is_logged_in is coming as STRING; standardize to boolean safely
  case
    when lower(cast(is_logged_in as string)) in ('true','t','1','yes','y') then true
    else false
  end as is_logged_in,

  cast(channel as string)                    as channel,
  cast(channel_source as string)             as channel_source,
  cast(medium as string)                     as medium,
  cast(utm_campaign as string)               as utm_campaign,
  cast(campaign_id as int64)                 as campaign_id,

  cast(device_type as string)                as device_type,
  cast(landing_page as string)               as landing_page,

  cast(page_views as int64)                  as page_views,
  cast(session_duration_seconds as int64)    as session_duration_seconds,

  -- integer flag -> boolean
  case when cast(bounced as int64) = 1 then true else false end as bounced,

  cast(source_system as string)              as source_system,
  cast(raw_file_name as string)              as raw_file_name

from {{ source('bronze', 'marketing_landing_page_sessions') }}