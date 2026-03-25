{{
  config(
    materialized='incremental',
    unique_key='month_of_the_year',
    incremental_predicates=["month_of_the_year >= date_trunc('month', current_date)"]
  )
}}

select
    date_trunc('month', payment_date_cleaned) as month_of_the_year,
    sum(amount) as total_revenue
from {{ ref('stg_membership_payments') }}

{% if is_incremental() %}
    where payment_date_cleaned >= (select max(month_of_the_year) from {{ this }})
{% endif %}

group by 1