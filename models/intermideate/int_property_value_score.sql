{{ config(materialized='view') }}

-- ============================================================
-- MODEL: int_property_value_score
--
-- Grain: One row per city
-- Business key: city
--
-- Purpose:
-- Evaluate city-level solar market potential based on
-- property values and residential construction years.
--
-- Scoring rules:
-- 1. Calculate the median valid WOZ property value.
-- 2. Assign 1 point when postcode WOZ exceeds the median.
-- 3. Assign 0 points otherwise.
-- 4. Calculate the proportion of homes built from 1995.
-- 5. Adjust the property value point by the eligible home ratio.
-- 6. Apply household weighting at city level.
--
-- Business assumptions:
-- Higher property values may indicate greater investment
-- capacity.
--
-- Homes built from 1995 are preferred under the defined
-- market screening criteria.
--
-- Construction year does not confirm solar installation
-- suitability.
--
-- WOZ represents the average property value of the entire
-- postcode, not only homes built from 1995.
--
-- Resident snapshot: 2022-01-01.
-- No temporal weighting is required.
--
-- Missing construction year counts are replaced with zero
-- for scoring calculations only.
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
        total_number_of_households,
        total_number_of_homes,

        number_of_homes_build_year_1995_to_2005,
        number_of_homes_build_year_2005_to_2015,
        number_of_homes_build_year_post_2015,

        average_woz_household_value_in_kEUR

    from {{ ref('stage_resident_household_data_raw') }}

), resident_preparation as (

    -- Calculate the number of eligible homes.
    -- Replace missing construction counts with zero.

    select
        zipcode,
        total_number_of_households,
        total_number_of_homes,
        average_woz_household_value_in_kEUR,

        coalesce(
            number_of_homes_build_year_1995_to_2005, 0
        )

        + coalesce(
            number_of_homes_build_year_2005_to_2015, 0
        )

        + coalesce(
            number_of_homes_build_year_post_2015, 0
        ) as eligible_home_count,

        -- Identify incomplete construction year data.

        case
            when number_of_homes_build_year_1995_to_2005
                 is null

              or number_of_homes_build_year_2005_to_2015
                 is null

              or number_of_homes_build_year_post_2015
                 is null

            then 1
            else 0
        end as has_incomplete_construction_data,

        -- Identify invalid construction year counts.

        case
            when number_of_homes_build_year_1995_to_2005 < 0

              or number_of_homes_build_year_2005_to_2015 < 0

              or number_of_homes_build_year_post_2015 < 0

            then 1
            else 0
        end as has_invalid_construction_data

    from resident_source

), eligible_home_calculation as (

    -- Calculate the proportion of eligible homes.
    -- Exclude invalid housing observations.

    select
        zipcode,
        total_number_of_households,
        total_number_of_homes,
        average_woz_household_value_in_kEUR,
        eligible_home_count,
        has_incomplete_construction_data,

        case
            when total_number_of_homes > 0

             and eligible_home_count >= 0

             and eligible_home_count <= total_number_of_homes

             and has_invalid_construction_data = 0

            then try_divide(
                eligible_home_count,
                total_number_of_homes
            )

            else null

        end as eligible_home_ratio

    from resident_preparation

), property_threshold as (

    -- Calculate one consistent WOZ threshold across
    -- valid postcodes with an unambiguous city mapping.

    select
        percentile_approx(
            r.average_woz_household_value_in_kEUR,
            0.5,
            10000
        ) as property_value_threshold

    from eligible_home_calculation as r

    inner join city_mapping as m
        on r.zipcode = m.zipcode

    where r.average_woz_household_value_in_kEUR > 0

      and r.eligible_home_ratio is not null

), property_value_scoring as (

    -- Apply binary property value scoring.

    select
        r.zipcode,
        r.total_number_of_households,
        r.total_number_of_homes,
        r.eligible_home_count,
        r.eligible_home_ratio,

        r.average_woz_household_value_in_kEUR,
        t.property_value_threshold,

        r.has_incomplete_construction_data,

        case
            when r.average_woz_household_value_in_kEUR
                 is null
            then null

            when r.average_woz_household_value_in_kEUR <= 0
            then null

            when t.property_value_threshold is null
            then null

            when r.average_woz_household_value_in_kEUR
                 > t.property_value_threshold
            then 1

            else 0

        end as property_value_point

    from eligible_home_calculation as r

    cross join property_threshold as t

), postcode_scoring as (

    -- Adjust the property value point using
    -- the eligible home ratio.

    select
        zipcode,
        total_number_of_households,
        total_number_of_homes,
        eligible_home_count,
        eligible_home_ratio,
        property_value_point,
        has_incomplete_construction_data,

        case
            when property_value_point is not null

             and eligible_home_ratio is not null

            then property_value_point
                 * eligible_home_ratio

            else null

        end as postcode_property_score

    from property_value_scoring

), household_weighting as (

    -- Apply household counts as postcode weights.
    -- Retain mapped postcodes for coverage monitoring.

    select
        m.zipcode,
        m.city,

        p.total_number_of_households,
        p.postcode_property_score,
        p.has_incomplete_construction_data,

        case
            when p.total_number_of_households > 0

             and p.postcode_property_score is not null

            then 1
            else 0
        end as is_valid_for_weighting,

        case
            when p.total_number_of_households > 0

             and p.postcode_property_score is not null

            then p.postcode_property_score
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

                 and has_incomplete_construction_data = 1

                then 1
                else 0
            end
        ) as incomplete_construction_data_zipcode_count,

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
        ) as property_value_score,

        -- Calculation components

        total_households,
        weighted_score_sum,

        -- Data quality and coverage

        total_zipcode_count,
        scored_zipcode_count,
        mapped_households,

        incomplete_construction_data_zipcode_count,

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