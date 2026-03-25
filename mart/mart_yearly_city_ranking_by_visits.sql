{{
  config(
    materialized='view')
}}
select
    extract(year from month_of_the_year) as calendar_year,
    city,
    sum(total_visits) as yearly_total_visits,
    dense_rank() over (order by yearly_total_visits desc) as city_rank
from {{ ref('mart_monthly_city_visits') }}
group by 1, 2
order by city_rank asc