{{ config(materialized='view') }}

-- Grain: one postcode at the source's fixed 2022-01-01 snapshot.
-- Remove any LIMIT 10 from the upstream staging model before building it.
-- User policy: numeric NULL inputs and numeric NULL outputs become zero.
-- was_zero_filled identifies missing source inputs imputed before calculation.

with source_filled as (

    select
        zipcode,
        city,
        date '2022-01-01' as snapshot_date,

        coalesce(total_occupants, 0)
            as total_occupants,

        coalesce(occupants_aged_25_to_45_yrs, 0)
            as occupants_aged_25_to_45_yrs,

        coalesce(occupants_aged_45_to_65_yrs, 0)
            as occupants_aged_45_to_65_yrs

    from {{ ref('int_Combined_SolarNowMarketData') }}

), age_counts as (

    select
        *,
        case
            when total_occupants > 0
             and occupants_aged_25_to_45_yrs >= 0
             and occupants_aged_45_to_65_yrs >= 0
             and occupants_aged_25_to_45_yrs
                 + occupants_aged_45_to_65_yrs <= total_occupants
            then occupants_aged_25_to_45_yrs
                 + occupants_aged_45_to_65_yrs
            else 0
        end as target_age_population
    from source_filled

), age_scored as (

 select
        *,
        coalesce(
            try_divide(target_age_population, total_occupants),
            0
        ) as target_age_ratio,

        case
            when total_occupants > 0
             and 3 * target_age_population > 2 * total_occupants
            then 1
            else 0
        end as age_structure_point

    from age_counts

)


select *
from age_scored
where age_structure_point = '1'