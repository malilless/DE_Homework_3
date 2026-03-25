{{
  config(
    materialized='view'
  )
}}
select
    service_id,
    customer_id,
    service_type,
    service_price,
    feedback_rate,
    TRY_CAST(service_usage_date as date) as service_usage_date_cleaned
from {{ ref('raw_services') }} 
where feedback_rate is not null