{{ config(
    materialized='table'
) }}

select *
from {{ ref('gyms') }}