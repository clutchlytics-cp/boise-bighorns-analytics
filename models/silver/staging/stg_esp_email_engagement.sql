{{ config(materialized='table') }}

select
  cast(engagement_id as int64)          as engagement_id,
  cast(campaign_id as int64)            as campaign_id,

  cast(_send_dt as date)                as send_dt,
  cast(_campaign_type as string)        as campaign_type,
  cast(_promo_code as string)           as promo_code,

  cast(marketing_customer_id as int64)  as marketing_customer_id,
  cast(email as string)                 as email,

  cast(opened as bool)                  as opened,
  cast(clicked as bool)                 as clicked,
  cast(converted as bool)               as converted,

  cast(source_system as string)         as source_system,
  cast(raw_file_name as string)         as raw_file_name

from {{ source('bronze', 'esp_email_engagement') }}