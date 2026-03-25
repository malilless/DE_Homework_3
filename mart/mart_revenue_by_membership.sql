{{
  config(
    materialized='incremental',
    unique_key='month_and_plan_id',
    incremental_predicates=["month_of_the_year >= date_trunc('month', current_date)"]
  )
}}

select
    date_trunc('month', p.payment_date_cleaned) || '-' || m.plan_type as month_and_plan_id,
    date_trunc('month', payment_date_cleaned) as month_of_the_year,
    plan_type,
    sum(amount) as total_revenue
from {{ ref('stg_membership_payments') }} as p
join {{ ref('stg_memberships') }} as m
    on p.membership_id = m.membership_id

{% if is_incremental() %}
    where p.payment_date_cleaned >= (select max(month_of_the_year) from {{ this }})
{% endif %}

group by 1, 2, 3