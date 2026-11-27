{{ config(materialized='view') }}

-- ============================================================
-- MODEL: int_ownership_score
--
-- Grain: One row per city
-- Business key: city
--
-- Purpose:
-- Evaluate city-level solar market potential based on
-- owner-occupied property percentages.
--
-- Scoring rules:
-- 1. Owner-occupied property percentage >= 50: assign 1 point.
-- 2. Owner-occupied property percentage < 50: assign 0 points.
-- 3. Apply household weighting at city level.
--
-- Business assumptions:
-- Owner-occupiers may have greater authority to make
-- decisions regarding solar panel installations.
--
-- The 50 percent threshold is a provisional assumption.
--
-- Resident snapshot: 2022-01-01.
-- No temporal weighting is required.
-- Missing or invalid ownership percentages are excluded.
-- ============================================================

with energy_source as (

    select
        zipcode,
        city,
        has_city_conflict

    from {{ ref('int_Combined_SolarNowMarketData') }}

), city_mapping as (

    -- Establish one unambiguous city per postcode.

    select
        zipcode,
        min(city) as city

    from energy_source

    group by zipcode

    having count(distinct city) = 1
       and max(has_city_conflict) = 0

), resident_source as (

    select
        zipcode,
        pct_owner_occupied_property,
        total_number_of_households

    from {{ ref('stage_resident_household_data_raw') }}

), postcode_scoring as (

    -- Apply binary ownership scoring at postcode level.
    -- Missing or invalid percentages return NULL.

    select
        zipcode,
        pct_owner_occupied_property,
        total_number_of_households,

        case
            when pct_owner_occupied_property is null
            then null

            when pct_owner_occupied_property < 0
              or pct_owner_occupied_property > 100
            then null

            when pct_owner_occupied_property >= 50
            then 1

            else 0
        end as postcode_ownership_score,

        case
            when pct_owner_occupied_property is null
            then 1
            else 0
        end as has_missing_ownership_data,

        case
            when pct_owner_occupied_property < 0
              or pct_owner_occupied_property > 100
            then 1
            else 0
        end as has_invalid_ownership_data

    from resident_source

), household_weighting as (

    -- Apply household counts as postcode weights.
    -- Retain mapped postcodes for coverage monitoring.

    select
        m.zipcode,
        m.city,

        p.total_number_of_households,
        p.postcode_ownership_score,

        p.has_missing_ownership_data,
        p.has_invalid_ownership_data,

        case
            when p.zipcode is null then 1
            else 0
        end as has_missing_resident_data,

        case
            when p.total_number_of_households > 0
             and p.postcode_ownership_score is not null
            then 1
            else 0
        end as is_valid_for_weighting,

        case
            when p.total_number_of_households > 0
             and p.postcode_ownership_score is not null
            then p.postcode_ownership_score
                 * p.total_number_of_households
            else 0
        end as household_weighted_score

    from city_mapping as m

    left join postcode_scoring as p
        on m.zipcode = p.zipcode

), city_aggregation as (

    -- Aggregate postcode scores to city level.

    select
        city,

        count(*) as total_zipcode_count,

        sum(is_valid_for_weighting)
            as scored_zipcode_count,

        sum(
            case
                when total_number_of_households > 0
                then cast(total_number_of_households as bigint)
                else 0
            end
        ) as mapped_households,

        sum(
            case
                when is_valid_for_weighting = 1
                then cast(total_number_of_households as bigint)
                else 0
            end
        ) as total_households,

        sum(
            case
                when has_missing_resident_data = 1
                  or has_missing_ownership_data = 1
                  or has_invalid_ownership_data = 1
                then 1
                else 0
            end
        ) as unavailable_ownership_data_zipcode_count,

        sum(household_weighted_score)
            as weighted_score_sum

    from household_weighting

    group by city

), final as (

    select
        city,

        -- Final business score

        try_divide(
            weighted_score_sum,
            total_households
        ) as ownership_score,

        -- Calculation components

        total_households,
        weighted_score_sum,

        -- Data quality and coverage

        total_zipcode_count,
        scored_zipcode_count,
        mapped_households,

        unavailable_ownership_data_zipcode_count,

        try_divide(
            scored_zipcode_count,
            total_zipcode_count
        ) as zipcode_score_coverage,

        try_divide(
            total_households,
            mapped_households
        ) as household_score_coverage

    from city_aggregation

)

select *
from final