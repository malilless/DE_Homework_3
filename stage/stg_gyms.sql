{{
  config(
    materialized='view'
  )
}}

with gyms_cleaned as (
    select
        gym_id,
        city,
        gym_name,
        TRY_CAST(opening_date as date) as opening_date_cleaned
    from {{ ref('raw_gyms') }} )

select * from gyms_cleaned
where opening_date_cleaned is not null