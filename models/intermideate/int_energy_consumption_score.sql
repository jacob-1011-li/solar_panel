{{ config(materialized='view') }}

-- ============================================================
-- MODEL: int_energy_consumption_score
--
-- Grain: One row per city
-- Business key: city
--
-- Purpose:
-- Measure relative energy consumption attractiveness
-- for solar market opportunity analysis.
--
-- Scoring rules:
-- 1. Calculate the median consumption for each year.
-- 2. Assign 1 when consumption exceeds the annual median.
-- 3. Assign 0 otherwise.
-- 4. Apply temporal weighting at postcode level.
-- 5. Apply household weighting at city level.
--
-- Business assumptions:
-- Higher energy consumption may indicate greater
-- potential for solar energy savings.
--
-- Household weights are based on the 2022 snapshot.
-- Missing energy observations are excluded from scoring.
-- ============================================================

with energy_source as (

    select
        zipcode,
        city,
        year,
        avg_annual_consume_per_connection,
        annual_consume_connection_count,
        has_city_conflict

    from {{ ref('int_Combined_SolarNowMarketData') }}

), resident_households as (

    select
        zipcode,
        total_number_of_households

    from {{ ref('stage_resident_household_data_raw') }}

), city_mapping as (

    -- Establish one unambiguous city per postcode.
    -- Exclude unresolved city conflicts.

    select
        zipcode,
        min(city) as city

    from energy_source

    group by zipcode

    having count(distinct city) = 1
       and max(has_city_conflict) = 0

), valid_energy as (

    -- Retain valid historical energy observations.

    select
        zipcode,
        year,
        avg_annual_consume_per_connection

    from energy_source

    where year between 2016 and 2020

      and avg_annual_consume_per_connection is not null

      and avg_annual_consume_per_connection >= 0

      and annual_consume_connection_count > 0

), annual_thresholds as (

    -- Calculate an independent median for each year.

    select
        year,

        percentile_approx(
            avg_annual_consume_per_connection,
            0.5,
            10000
        ) as annual_consumption_threshold

    from valid_energy

    group by year

), annual_scoring as (

    -- Apply binary scoring at postcode-year level.

    select
        e.zipcode,
        e.year,
        e.avg_annual_consume_per_connection,
        t.annual_consumption_threshold,

        case
            when e.avg_annual_consume_per_connection
                 > t.annual_consumption_threshold
            then 1
            else 0
        end as annual_energy_point,

        -- Temporal weights:
        -- 2016 = 1
        -- 2017 = 2
        -- 2018 = 3
        -- 2019 = 4
        -- 2020 = 5

        e.year - 2015 as year_weight

    from valid_energy as e

    inner join annual_thresholds as t
        on e.year = t.year

), postcode_scoring as (

    -- Consolidate historical scores into one postcode score.

    select
        zipcode,

        count(distinct year)
            as available_energy_year_count,

        sum(annual_energy_point * year_weight)
            as temporal_weighted_score_sum,

        sum(year_weight)
            as total_year_weight,

        try_divide(
            sum(annual_energy_point * year_weight),
            sum(year_weight)
        ) as postcode_energy_score

    from annual_scoring

    group by zipcode

), household_weighting as (

    -- Join postcode-level scores to household counts.
    -- Keep mapped postcodes with missing scoring inputs
    -- for coverage monitoring.

    select
        m.zipcode,
        m.city,

        r.total_number_of_households,

        p.available_energy_year_count,
        p.postcode_energy_score,

        case
            when r.total_number_of_households > 0
             and p.postcode_energy_score is not null
            then 1
            else 0
        end as is_valid_for_weighting,

        case
            when r.total_number_of_households > 0
             and p.postcode_energy_score is not null
            then p.postcode_energy_score
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
                 and available_energy_year_count < 5
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
        ) as energy_consumption_score,

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
order by household_score_coverage asc