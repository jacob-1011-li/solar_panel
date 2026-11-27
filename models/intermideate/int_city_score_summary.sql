{{ config(materialized='view') }}

-- ============================================================
-- MODEL: int_city_score_summary
--
-- Grain: One row per city
-- Business key: city
--
-- Purpose:
-- Consolidate six independent city-level scoring models
-- to calculate the overall solar market opportunity score.
--
-- Scoring factors:
-- 1. Energy consumption
-- 2. Delivery percentage
-- 3. Smart meter adoption
-- 4. Age structure
-- 5. Property ownership
-- 6. Property value and construction year
--
-- Scoring methodology:
-- Each factor has a score between 0 and 1.
-- All six factors receive equal weighting.
-- The total market score ranges from 0 to 6.
--
-- Null handling:
-- Missing scores are retained as NULL.
-- The total market score is NULL when any factor is missing.
--
-- Business assumptions:
-- Equal weighting is used as the baseline scoring method.
-- Final business weights remain subject to validation.
-- ============================================================

with energy_consumption as (

    select
        city,
        energy_consumption_score

    from {{ ref('int_energy_consumption_score') }}

), delivery as (

    select
        city,
        delivery_score

    from {{ ref('int_delivery_score') }}

), smartmeter as (

    select
        city,
        smartmeter_score

    from {{ ref('int_smartmeter_score') }}

), age_structure as (

    select
        city,
        age_structure_score

    from {{ ref('int_age_structure_score') }}

), ownership as (

    select
        city,
        ownership_score

    from {{ ref('int_ownership_score') }}

), property_value as (

    select
        city,
        property_value_score

    from {{ ref('int_property_value_score') }}

), city_population as (

    -- Establish the complete city population.
    -- UNION removes duplicate city names.

    select city from energy_consumption

    union

    select city from delivery

    union

    select city from smartmeter

    union

    select city from age_structure

    union

    select city from ownership

    union

    select city from property_value

), combined_scores as (

    -- Consolidate all six scores at city level.

    select
        c.city,

        e.energy_consumption_score,
        d.delivery_score,
        s.smartmeter_score,
        a.age_structure_score,
        o.ownership_score,
        p.property_value_score

    from city_population as c

    left join energy_consumption as e
        on c.city = e.city

    left join delivery as d
        on c.city = d.city

    left join smartmeter as s
        on c.city = s.city

    left join age_structure as a
        on c.city = a.city

    left join ownership as o
        on c.city = o.city

    left join property_value as p
        on c.city = p.city

), final as (

    select
        city,

        -- Energy factors

        energy_consumption_score,
        delivery_score,
        smartmeter_score,

        -- Resident factors

        age_structure_score,
        ownership_score,
        property_value_score,

        -- Final market opportunity score

        (
            energy_consumption_score
            + delivery_score
            + smartmeter_score
            + age_structure_score
            + ownership_score
            + property_value_score

        ) as total_market_score

    from combined_scores

)

select *
from final