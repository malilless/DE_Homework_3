{{
  config(
    materialized='view')
}}

with payments_cleaned as (
    select
        payment_id,
        customer_id,
        membership_id,
        amount,
        payment_method,
        TRY_CAST(payment_date as date) as payment_date_cleaned
    from {{ ref('raw_membership_payments') }}
    where payment_status != 'error')

select * from payments_cleaned
where payment_date_cleaned is not null