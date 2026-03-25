{{ config(
    materialized='table'
) }}

select *
from {{ ref('membership_payments') }}