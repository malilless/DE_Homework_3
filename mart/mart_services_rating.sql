{{
  config(
    materialized='incremental',
    unique_key='month_and_service_id',
    incremental_predicates=["month_of_the_year >= date_trunc('month', current_date)"]
  )
}}
  
select
    date_trunc('month', service_usage_date_cleaned) || '-' || service_type as month_and_service_id,
    date_trunc('month', service_usage_date_cleaned) as month_of_the_year,
    service_type,
    avg(feedback_rate) as avg_rating,
    count(customer_id) as total_clients
from {{ ref('stg_services') }}

{% if is_incremental() %}
    where service_usage_date_cleaned >= (select max(month_of_the_year) from {{ this }})
{% endif %}

group by 1, 2, 3