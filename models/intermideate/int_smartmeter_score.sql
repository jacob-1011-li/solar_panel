{{ config(materialized='view') }}

-- ============================================================
-- MODEL: int_smartmeter_score
--
-- Grain: One row per city
-- Business key: city
--
-- Purpose:
-- Evaluate city-level smart meter adoption as an indicator
-- of energy infrastructure maturity.
--
-- Scoring rules:
-- 1. Smart meter percentage >= 50: assign 1 point.
-- 2. Smart meter percentage < 50: assign 0 points.
-- 3. Apply temporal weighting at postcode level.
-- 4. Apply household weighting at city level.
--
-- Business assumptions:
-- Higher smart meter adoption may indicate greater
-- energy infrastructure maturity.
--
-- Smart meter adoption is not a direct measure of
-- solar panel purchasing intention.
--
-- The 50 percent threshold is a provisional assumption.
-- Household weights use the 2022 snapshot.
-- Missing or invalid observations are excluded.
-- ============================================================

with energy_source as (

    select
        zipcode,
        city,
        year,
        smartmeter_pct,
        smartmeter_pct_connection_count,
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

), valid_smartmeter as (

    -- Retain valid historical smart meter observations.

    select
        zipcode,
        year,
        smartmeter_pct

    from energy_source

    where year between 2016 and 2020

      and smartmeter_pct is not null

      and smartmeter_pct between 0 and 100

      and smartmeter_pct_connection_count > 0

), annual_scoring as (

    -- Apply binary scoring at postcode-year level.

    select
        zipcode,
        year,
        smartmeter_pct,

        case
            when smartmeter_pct >= 50 then 1
            else 0
        end as annual_smartmeter_point,

        -- Temporal weights:
        -- 2016 = 1
        -- 2017 = 2
        -- 2018 = 3
        -- 2019 = 4
        -- 2020 = 5

        year - 2015 as year_weight

    from valid_smartmeter

), postcode_scoring as (

    -- Consolidate annual scores into one postcode score.

    select
        zipcode,

        count(distinct year)
            as available_smartmeter_year_count,

        sum(annual_smartmeter_point * year_weight)
            as temporal_weighted_score_sum,

        sum(year_weight)
            as total_year_weight,

        try_divide(
            sum(annual_smartmeter_point * year_weight),
            sum(year_weight)
        ) as postcode_smartmeter_score

    from annual_scoring

    group by zipcode

), household_weighting as (

    -- Apply household counts as postcode weights.
    -- Retain mapped postcodes for coverage monitoring.

    select
        m.zipcode,
        m.city,

        r.total_number_of_households,

        p.available_smartmeter_year_count,
        p.postcode_smartmeter_score,

        case
            when r.total_number_of_households > 0
             and p.postcode_smartmeter_score is not null
            then 1
            else 0
        end as is_valid_for_weighting,

        case
            when r.total_number_of_households > 0
             and p.postcode_smartmeter_score is not null
            then p.postcode_smartmeter_score
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
                 and available_smartmeter_year_count < 5
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
        ) as smartmeter_score,

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
--order by smartmeter_score desc