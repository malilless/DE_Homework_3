{{
  config(
    materialized='incremental',
    unique_key='month_and_city_id',
    incremental_predicates=["month_of_the_year >= date_trunc('month', current_date)"]
  )
}}

select
    date_trunc('month', v.visit_date_cleaned) || '-' || g.city as month_and_city_id,
    date_trunc('month', visit_date_cleaned) as month_of_the_year,
    g.city,
    count(v.visit_id) as total_visits
from {{ ref('stg_visits') }} as v
join {{ ref('stg_gyms') }} as g
    on v.gym_id = g.gym_id

{% if is_incremental() %}
    where visit_date_cleaned >= (select max(month_of_the_year) from {{ this }})
{% endif %}

group by 1, 2, 3

