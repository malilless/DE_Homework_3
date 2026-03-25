{{
  config(
    materialized='view')
}}
select
    vs.first_name,
    vs.last_name,
    ms.total_memberships,
    vs.activity_rank
from {{ref('mart_customer_visit_statistics')}} as vs
left join {{ref('mart_customer_membership_statistics')}} as ms
    on vs.customer_id = ms.customer_id
order by vs.activity_rank
limit 10