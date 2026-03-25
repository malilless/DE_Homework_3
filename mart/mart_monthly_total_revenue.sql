{{
  config(
    materialized='view')
}}
with grand_total_revenue_data as (
    select
        month_of_the_year,
        total_revenue
        from {{ ref('mart_monthly_membership_revenue') }}
    union all
    select
        month_of_the_year,
        total_revenue
        from {{ ref('mart_monthly_services_revenue') }} )
select
    month_of_the_year,
    sum(total_revenue) as grand_total_revenue
    from grand_total_revenue_data
    group by month_of_the_year