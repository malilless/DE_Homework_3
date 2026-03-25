{{
  config(
    materialized='view'
  )
}}

with visits_cleaned as (
    select
        visit_id,
        customer_id,
        gym_id,
        TRY_CAST(entrance_time as time) as entrance_time_cleaned,
        TRY_CAST(exit_time as time) as exit_time_cleaned,
        TRY_CAST(visit_date as date) as visit_date_cleaned
    from {{ ref('raw_visits') }} )

select * from visits_cleaned
where entrance_time_cleaned is not null
    and exit_time_cleaned is not null
    and visit_date_cleaned is not null