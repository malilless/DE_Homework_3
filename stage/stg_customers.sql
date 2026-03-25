{{
  config(
    materialized='view'
  )
}}

with cutomers_cleaned as (
    select
        customer_id,
        first_name,
        last_name,
        TRY_CAST(signup_date as date) as signup_date_cleaned
    from {{ ref('raw_customers') }} )

select * from cutomers_cleaned
where signup_date_cleaned is not null