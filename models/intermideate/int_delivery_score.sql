{{ config(materialized='view') }}

-- ============================================================
-- MODEL: int_delivery_score
--
-- Grain: One row per city
-- Business key: city
--
-- Purpose:
-- Identify potential markets for new solar installations
-- using historical delivery percentages.
--
-- Scoring rules:
-- 1. Delivery percentage >= 95: assign 1 point.
-- 2. Delivery percentage < 95: assign 0 points.
-- 3. Apply temporal weighting at postcode level.
-- 4. Apply household weighting at city level.
--
-- Business assumptions:
-- Higher delivery percentages may indicate less existing
-- energy feedback to the grid.
--
-- Delivery percentage is not a direct measure of
-- solar panel adoption.
--
-- The 95 percent threshold is a provisional assumption.
-- Household weights use the 2022 snapshot.
-- Missing or invalid delivery observations are excluded.
-- ============================================================

with energy_source as (

    select
        zipcode,
        city,
        year,
        delivery_pct,
        delivery_pct_connection_count,
        has_city_conflict

    from {{ ref('int_Combined_SolarNowMarketData') }}

), resident_households as (

    select
        zipcode,
        total_number_of_households

    from {{ ref('stage_resident_household_data_raw') }}

), city_mapping as (

    -- Establish one unambiguous city per postcode.

    select
        zipcode,
        min(city) as city

    from energy_source

    group by zipcode

    having count(distinct city) = 1
       and max(has_city_conflict) = 0

), valid_delivery as (

    -- Retain valid historical delivery observations.

    select
        zipcode,
        year,
        delivery_pct

    from energy_source

    where year between 2016 and 2020

      and delivery_pct is not null

      and delivery_pct between 0 and 100

      and delivery_pct_connection_count > 0

), annual_scoring as (

    -- Apply binary scoring at postcode-year level.

    select
        zipcode,
        year,
        delivery_pct,

        case
            when delivery_pct >= 95 then 1
            else 0
        end as annual_delivery_point,

        -- Temporal weights:
        -- 2016 = 1
        -- 2017 = 2
        -- 2018 = 3
        -- 2019 = 4
        -- 2020 = 5

        year - 2015 as year_weight

    from valid_delivery

), postcode_scoring as (

    -- Consolidate annual scores into one postcode score.

    select
        zipcode,

        count(distinct year)
            as available_delivery_year_count,

        sum(annual_delivery_point * year_weight)
            as temporal_weighted_score_sum,

        sum(year_weight)
            as total_year_weight,

        try_divide(
            sum(annual_delivery_point * year_weight),
            sum(year_weight)
        ) as postcode_delivery_score

    from annual_scoring

    group by zipcode

), household_weighting as (

    -- Apply household counts as postcode weights.
    -- Retain mapped postcodes for coverage monitoring.

    select
        m.zipcode,
        m.city,

        r.total_number_of_households,

        p.available_delivery_year_count,
        p.postcode_delivery_score,

        case
            when r.total_number_of_households > 0
             and p.postcode_delivery_score is not null
            then 1
            else 0
        end as is_valid_for_weighting,

        case
            when r.total_number_of_households > 0
             and p.postcode_delivery_score is not null
            then p.postcode_delivery_score
                 * r.total_number_of_households
            else 0
        end as household_weighted_score

    from city_mapping as m

    left join postcode_scoring as p
        on m.zipcode = p.zipcode

    left join resident_households as r
        on m.zipcode = r.zipcode

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
                when is_valid_for_weighting = 1
                 and available_delivery_year_count < 5
                then 1
                else 0
            end
        ) as incomplete_history_zipcode_count,

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
        ) as delivery_score,

        -- Calculation components

        total_households,
        weighted_score_sum,

        -- Data quality and coverage

        total_zipcode_count,
        scored_zipcode_count,
        mapped_households,

        incomplete_history_zipcode_count,

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