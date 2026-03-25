{{ config(
    materialized='table'
) }}

select *
from {{ ref('memberships') }}