{{ config(materialized='view') }}

-- ============================================================
-- MODEL: int_age_structure_score
--
-- Grain: One row per city
-- Business key: city
--
-- Purpose:
-- Evaluate city-level market potential based on the
-- proportion of residents within the target age group.
--
-- Target age group:
-- 25 to 65 years.
--
-- Scoring rules:
-- 1. Target age ratio > 2/3: assign 1 point.
-- 2. Target age ratio <= 2/3: assign 0 points.
-- 3. Apply household weighting at city level.
--
-- Business assumptions:
-- Residents within the target age group may have greater
-- potential for household investment.
--
-- Resident snapshot: 2022-01-01.
-- No temporal weighting is required.
--
-- Missing age counts are treated as zero for calculation
-- when at least one target age count is available.
-- Records with both target age counts missing are excluded.
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
        total_occupants,
        occupants_aged_25_to_45_yrs,
        occupants_aged_45_to_65_yrs,
        total_number_of_households

    from {{ ref('stage_resident_household_data_raw') }}

), resident_preparation as (

    -- Calculate the target age population.
    -- Retain missing age information for DQ monitoring.

    select
        zipcode,
        total_occupants,
        total_number_of_households,

        coalesce(occupants_aged_25_to_45_yrs, 0)
        + coalesce(occupants_aged_45_to_65_yrs, 0)
            as target_age_population,

        case
            when occupants_aged_25_to_45_yrs is null
              or occupants_aged_45_to_65_yrs is null
            then 1
            else 0
        end as has_incomplete_age_data,

        case
            when occupants_aged_25_to_45_yrs is null
             and occupants_aged_45_to_65_yrs is null
            then 1
            else 0
        end as has_missing_target_age_data,

        case
            when occupants_aged_25_to_45_yrs < 0
              or occupants_aged_45_to_65_yrs < 0
            then 1
            else 0
        end as has_invalid_age_data

    from resident_source

), age_ratio_calculation as (

    -- Calculate a valid target age population ratio.

    select
        zipcode,
        total_occupants,
        total_number_of_households,
        target_age_population,
        has_incomplete_age_data,

        case
            when total_occupants > 0
             and has_missing_target_age_data = 0
             and has_invalid_age_data = 0
             and target_age_population <= total_occupants
            then try_divide(
                target_age_population,
                total_occupants
            )
            else null
        end as target_age_ratio

    from resident_preparation

), postcode_scoring as (

    -- Apply binary scoring at postcode level.

    select
        zipcode,
        total_occupants,
        total_number_of_households,
        target_age_population,
        target_age_ratio,
        has_incomplete_age_data,

        case
            when target_age_ratio is null then null
            when target_age_ratio > try_divide(2, 3) then 1
            else 0
        end as postcode_age_score

    from age_ratio_calculation

), household_weighting as (

    -- Apply household counts as postcode weights.
    -- Retain mapped postcodes for coverage monitoring.

    select
        m.zipcode,
        m.city,

        p.total_number_of_households,
        p.postcode_age_score,
        p.has_incomplete_age_data,

        case
            when p.total_number_of_households > 0
             and p.postcode_age_score is not null
            then 1
            else 0
        end as is_valid_for_weighting,

        case
            when p.total_number_of_households > 0
             and p.postcode_age_score is not null
            then p.postcode_age_score
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
                when is_valid_for_weighting = 1
                 and has_incomplete_age_data = 1
                then 1
                else 0
            end
        ) as incomplete_age_data_zipcode_count,

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
        ) as age_structure_score,

        -- Calculation components

        total_households,
        weighted_score_sum,

        -- Data quality and coverage

        total_zipcode_count,
        scored_zipcode_count,
        mapped_households,

        incomplete_age_data_zipcode_count,

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
order by age_structure_score desc