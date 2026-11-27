{{ config(materialized='view') }}

-- ============================================================
-- MODEL: int_Combined_SolarNowMarketData
--
-- Grain: One row per zipcode per year
-- Business Key: zipcode + year
--
-- Purpose:
-- Combine Full and Subset energy market datasets.
-- Standardize business attributes.
-- Remove exact duplicate records.
-- Aggregate energy metrics to postcode-year level.
-- Preserve source lineage and data quality information.
--
-- Note:
-- Connection-based weighting is a provisional assumption.
-- Business scoring and temporal weighting are performed
-- in downstream intermediate models.
-- ============================================================

with appended as (

    select
        net_manager,
        purchase_area,
        street,
        zipcode,
        city,
        delivery_pct,
        pct_of_active_connections,
        type_conn_pct,
        type_of_connection,
        annual_consume,
        annual_consume_lowtarif_pct,
        smartmeter_pct,
        num_connection,
        year,
        'full' as source_dataset

    from {{ ref('stage_SolarNowMarketData_Full_raw') }}

    union all

    select
        net_manager,
        purchase_area,
        street,
        zipcode,
        city,
        delivery_pct,
        pct_of_active_connections,
        type_conn_pct,
        type_of_connection,
        annual_consume,
        annual_consume_lowtarif_pct,
        smartmeter_pct,
        num_connection,
        year,
        'subset' as source_dataset

    from {{ ref('stage_SolarNowMarketData_Subset_raw') }}

), standardized as (

    select
        case
            when net_manager in (
                'LIANDER',
                'LIANDER N.V',
                'LIANDER N.V.',
                'LIANDER N.V (NW)',
                'LIANDER N.V. (NW)',
                'LIANDER N.V (ZW)',
                'LIANDER N.V. (ZW)'
            ) then 'LIANDER'
            else net_manager
        end as net_manager,

        purchase_area,
        street,
        zipcode,

        case
            when regexp_replace(city, '[^A-Z]', '')
                = 'SGRAVENHAGE'
            then concat(char(39), 'S-GRAVENHAGE')
            else city
        end as city,

        delivery_pct,
        pct_of_active_connections,
        type_conn_pct,
        upper(type_of_connection) as type_of_connection,
        annual_consume,
        annual_consume_lowtarif_pct,
        smartmeter_pct,
        num_connection,
        year,
        source_dataset

    from appended

), valid_keys as (

    select *
    from standardized

    where zipcode is not null
      and year is not null

), source_coverage as (

    -- Capture source lineage before deduplication.

    select
        zipcode,
        year,

        count(*) as raw_source_row_count,

        count(distinct source_dataset)
            as source_dataset_count,

        max(
            case
                when source_dataset = 'full' then 1
                else 0
            end
        ) as has_full_source,

        max(
            case
                when source_dataset = 'subset' then 1
                else 0
            end
        ) as has_subset_source

    from valid_keys

    group by zipcode, year

), ranked as (

    -- Prefer Full when all business fields are identical.

    select
        *,

        row_number() over (

            partition by
                net_manager,
                purchase_area,
                street,
                zipcode,
                city,
                delivery_pct,
                pct_of_active_connections,
                type_conn_pct,
                type_of_connection,
                annual_consume,
                annual_consume_lowtarif_pct,
                smartmeter_pct,
                num_connection,
                year

            order by
                case
                    when source_dataset = 'full' then 1
                    else 2
                end

        ) as duplicate_rank

    from valid_keys

), deduplicated as (

    select *
    from ranked
    where duplicate_rank = 1

), zipcode_year_totals as (

    select
        zipcode,
        year,

        -- City mapping

        case
            when count(distinct city) = 1
            then min(city)
            else null
        end as city,

        count(distinct city) as city_name_count,

        count(
            case when city is null then 1 end
        ) as missing_city_row_count,

        count(distinct net_manager)
            as net_manager_count,

        count(*) as source_row_count,

        -- Connection volume

        sum(
            case
                when num_connection >= 0
                then cast(num_connection as bigint)
            end
        ) as covered_connection_count,

        -- Annual consumption

        sum(
            case
                when annual_consume is not null
                 and num_connection > 0
                then annual_consume * num_connection
            end
        ) as annual_consume_weighted_sum,

        sum(
            case
                when annual_consume is not null
                 and num_connection > 0
                then cast(num_connection as bigint)
                else 0
            end
        ) as annual_consume_connection_count,

        -- Delivery percentage

        sum(
            case
                when delivery_pct between 0 and 100
                 and num_connection > 0
                then delivery_pct * num_connection
            end
        ) as delivery_pct_weighted_sum,

        sum(
            case
                when delivery_pct between 0 and 100
                 and num_connection > 0
                then cast(num_connection as bigint)
                else 0
            end
        ) as delivery_pct_connection_count,

        -- Active connections percentage

        sum(
            case
                when pct_of_active_connections between 0 and 100
                 and num_connection > 0
                then pct_of_active_connections * num_connection
            end
        ) as active_connections_pct_weighted_sum,

        sum(
            case
                when pct_of_active_connections between 0 and 100
                 and num_connection > 0
                then cast(num_connection as bigint)
                else 0
            end
        ) as active_connections_pct_connection_count,

        -- Low tariff consumption percentage

        sum(
            case
                when annual_consume_lowtarif_pct between 0 and 100
                 and num_connection > 0
                then annual_consume_lowtarif_pct * num_connection
            end
        ) as lowtarif_pct_weighted_sum,

        sum(
            case
                when annual_consume_lowtarif_pct between 0 and 100
                 and num_connection > 0
                then cast(num_connection as bigint)
                else 0
            end
        ) as lowtarif_pct_connection_count,

        -- Smart meter percentage

        sum(
            case
                when smartmeter_pct between 0 and 100
                 and num_connection > 0
                then smartmeter_pct * num_connection
            end
        ) as smartmeter_pct_weighted_sum,

        sum(
            case
                when smartmeter_pct between 0 and 100
                 and num_connection > 0
                then cast(num_connection as bigint)
                else 0
            end
        ) as smartmeter_pct_connection_count

    from deduplicated

    group by zipcode, year

), final as (

    select

        -- Business keys and geographic attributes

        z.zipcode,
        z.city,
        z.year,

        -- Core business metrics

        try_divide(
            z.annual_consume_weighted_sum,
            z.annual_consume_connection_count
        ) as avg_annual_consume_per_connection,

        try_divide(
            z.delivery_pct_weighted_sum,
            z.delivery_pct_connection_count
        ) as delivery_pct,

        try_divide(
            z.smartmeter_pct_weighted_sum,
            z.smartmeter_pct_connection_count
        ) as smartmeter_pct,

        -- Supporting business metrics

        z.covered_connection_count,

        try_divide(
            z.active_connections_pct_weighted_sum,
            z.active_connections_pct_connection_count
        ) as pct_of_active_connections,

        try_divide(
            z.lowtarif_pct_weighted_sum,
            z.lowtarif_pct_connection_count
        ) as annual_consume_lowtarif_pct,

        -- Technical calculation fields

        z.annual_consume_weighted_sum,
        z.annual_consume_connection_count,

        z.delivery_pct_weighted_sum,
        z.delivery_pct_connection_count,

        z.smartmeter_pct_weighted_sum,
        z.smartmeter_pct_connection_count,

        z.active_connections_pct_weighted_sum,
        z.active_connections_pct_connection_count,

        z.lowtarif_pct_weighted_sum,
        z.lowtarif_pct_connection_count,

        -- Source lineage

        s.raw_source_row_count,
        z.source_row_count,

        s.raw_source_row_count - z.source_row_count
            as duplicate_row_count,

        s.source_dataset_count,
        s.has_full_source,
        s.has_subset_source,

        -- Data quality indicators

        z.city_name_count,

        case
            when z.city_name_count > 1 then 1
            else 0
        end as has_city_conflict,

        z.missing_city_row_count,
        z.net_manager_count,

        case
            when z.source_row_count > 1 then 1
            else 0
        end as has_multiple_source_rows

    from zipcode_year_totals as z

    inner join source_coverage as s
        on z.zipcode = s.zipcode
        and z.year = s.year

)

select *
from final