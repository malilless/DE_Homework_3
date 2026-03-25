{{ config(
    materialized='table'
) }}

select *
from {{ ref('services') }}