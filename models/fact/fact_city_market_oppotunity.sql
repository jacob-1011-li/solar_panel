{{ config(materialized='table') }}

with city_score_summary as (

    select
        city,
        total_market_score

    from {{ ref('int_city_score_summary') }}

), final as (

    select
        city,
        total_market_score

    from city_score_summary

)

select *
from final
order by total_market_score desc