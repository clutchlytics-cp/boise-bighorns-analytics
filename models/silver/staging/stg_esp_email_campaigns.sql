{{ config(materialized='table') }}

select
  cast(campaign_id as int64)        as campaign_id,
  cast(campaign_type as string)     as campaign_type,
  cast(campaign_name as string)     as campaign_name,

  cast(send_date as date)           as send_dt,
  cast(channel as string)           as channel,
  cast(subject_line as string)      as subject_line,

  cast(promo_code as string)        as promo_code,
  cast(offer_type as string)        as offer_type,
  cast(discount_value as numeric)   as discount_value,

  cast(utm_campaign as string)      as utm_campaign,

  cast(source_system as string)     as source_system,
  cast(source_Raw_file as string)   as raw_file_name

from {{ source('bronze', 'esp_email_campaigns') }}