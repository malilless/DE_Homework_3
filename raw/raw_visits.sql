{{ config(
    materialized='table'
) }}

select *
from {{ ref('visits') }}