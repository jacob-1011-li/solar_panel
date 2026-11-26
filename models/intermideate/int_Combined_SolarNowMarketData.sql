{{ config(materialized='view') }}

-- Grain: one source-reported city name per source year.
-- Preserve the existing detail model separately for lineage and reconciliation.
-- Exact-row deduplication does not resolve overlapping source coverage.

with appended as (

    select
        net_manager, purchase_area, street, zipcode, city,
        delivery_pct, pct_of_active_connections,
        type_conn_pct, type_of_connection, annual_consume,
        annual_consume_lowtarif_pct, smartmeter_pct,
        num_connection, year,
        'full' as source_dataset
    from {{ ref('stage_SolarNowMarketData_Full_raw') }}

    union all

    select
        net_manager, purchase_area, street, zipcode, city,
        delivery_pct, pct_of_active_connections,
        type_conn_pct, type_of_connection, annual_consume,
        annual_consume_lowtarif_pct, smartmeter_pct,
        num_connection, year,
        'subset' as source_dataset
    from {{ ref('stage_SolarNowMarketData_Subset_raw') }}

), standardized as (

    select
        case
            when upper(trim(net_manager)) in (
                'LIANDER',
                'LIANDER N.V',
                'LIANDER N.V.',
                'LIANDER N.V (NW)',
                'LIANDER N.V. (NW)',
                'LIANDER N.V (ZW)',
                'LIANDER N.V. (ZW)'
            ) then 'LIANDER'
            else upper(trim(net_manager))
        end as net_manager,
        purchase_area,
        street,
        zipcode,
        case
    when regexp_replace(
        upper(trim(city)),
        '[^A-Z]',
        ''
    ) = 'SGRAVENHAGE'
        then concat(char(39), 'S-GRAVENHAGE')
        else nullif(upper(trim(city)), '')
        end as city,
        -- Recognize verified 's-Gravenhage aliases, including a corrupt separator.
        -- The regex is only a lookup key for this city; other names are preserved.
        
        delivery_pct,
        pct_of_active_connections,
        type_conn_pct,
        upper(trim(type_of_connection)) as type_of_connection,
        annual_consume,
        annual_consume_lowtarif_pct,
        smartmeter_pct,
        num_connection,
        year,
        source_dataset
    from appended

), ranked as (

    select
        *,
        row_number() over (
            partition by
                net_manager, purchase_area, street, zipcode, city,
                delivery_pct, pct_of_active_connections,
                type_conn_pct, type_of_connection, annual_consume,
                annual_consume_lowtarif_pct, smartmeter_pct,
                num_connection, year
            order by
                case when source_dataset = 'full' then 1 else 2 end
        ) as duplicate_rank
    from standardized

), deduplicated as (

    select *
    from ranked
    where duplicate_rank = 1

), city_year_totals as (

    select
        city,
        year,
        count(*) as source_row_count,
        -- Count of recorded zipcode labels, not proven full postcode coverage.
        count(distinct zipcode) as recorded_postcode_count,
        sum(case when num_connection >= 0
            then cast(num_connection as bigint) end) as covered_connection_count,

        -- Retain each metric's numerator and its own valid denominator.
        -- This product is a weighting component, not verified metered energy.
        sum(case when annual_consume is not null and num_connection > 0
            then annual_consume * num_connection end) as annual_consume_weighted_sum,
        sum(case when annual_consume is not null and num_connection > 0
            then cast(num_connection as bigint) else 0 end) as annual_consume_connection_count,

        -- Source percentage scale is 0 to 100; invalid/missing values are excluded.
        sum(case when delivery_pct between 0 and 100 and num_connection > 0
            then delivery_pct * num_connection end) as delivery_pct_weighted_sum,
        sum(case when delivery_pct between 0 and 100 and num_connection > 0
            then cast(num_connection as bigint) else 0 end) as delivery_pct_connection_count,

        sum(case when pct_of_active_connections between 0 and 100 and num_connection > 0
            then pct_of_active_connections * num_connection end) as active_connections_pct_weighted_sum,
        sum(case when pct_of_active_connections between 0 and 100 and num_connection > 0
            then cast(num_connection as bigint) else 0 end) as active_connections_pct_connection_count,

        sum(case when annual_consume_lowtarif_pct between 0 and 100 and num_connection > 0
            then annual_consume_lowtarif_pct * num_connection end) as lowtarif_pct_weighted_sum,
        sum(case when annual_consume_lowtarif_pct between 0 and 100 and num_connection > 0
            then cast(num_connection as bigint) else 0 end) as lowtarif_pct_connection_count,

        sum(case when smartmeter_pct between 0 and 100 and num_connection > 0
            then smartmeter_pct * num_connection end) as smartmeter_pct_weighted_sum,
        sum(case when smartmeter_pct between 0 and 100 and num_connection > 0
            then cast(num_connection as bigint) else 0 end) as smartmeter_pct_connection_count

    from deduplicated
    -- Keep unassignable records in the detail model; do not merge them into a city.
    where city is not null
      and year is not null
    group by city, year

)

select
    city,
    year,
    source_row_count,
    recorded_postcode_count,
    covered_connection_count,

    annual_consume_weighted_sum,
    annual_consume_connection_count,
    try_divide(annual_consume_weighted_sum, annual_consume_connection_count)
        as avg_annual_consume_per_connection,

    delivery_pct_weighted_sum,
    delivery_pct_connection_count,
    try_divide(delivery_pct_weighted_sum, delivery_pct_connection_count)
        as delivery_pct,

    active_connections_pct_weighted_sum,
    active_connections_pct_connection_count,
    try_divide(active_connections_pct_weighted_sum, active_connections_pct_connection_count)
        as pct_of_active_connections,

    lowtarif_pct_weighted_sum,
    lowtarif_pct_connection_count,
    try_divide(lowtarif_pct_weighted_sum, lowtarif_pct_connection_count)
        as annual_consume_lowtarif_pct,

    smartmeter_pct_weighted_sum,
    smartmeter_pct_connection_count,
    try_divide(smartmeter_pct_weighted_sum, smartmeter_pct_connection_count)
        as smartmeter_pct

from city_year_totals
order by city, year
