{{
  config(
    materialized='table'
  )
}}

with customer_visits as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        count(v.visit_id) as total_visits
    from {{ ref('stg_visits') }} as v
    join {{ ref('stg_customers') }} as c
        on v.customer_id = c.customer_id
    group by 1, 2, 3
)

select
    *,
    dense_rank() over (order by total_visits desc) as activity_rank
from customer_visits
order by activity_rank asc