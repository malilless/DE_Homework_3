{{
  config(
    materialized='table'
  )
}}

select
    c.customer_id,
    c.first_name,
    c.last_name,
    count(m.membership_id) as total_memberships,
from {{ ref('stg_memberships') }} as m
join {{ ref('stg_customers') }} as c
    on m.customer_id = c.customer_id
group by 1, 2, 3
