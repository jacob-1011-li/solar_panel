with source_data as (
    select * 
    from {{ source ('solar_panel', 'SolarNowMarketData_Subset_raw')}}
),

rename_and_casted as (
    select
        nullif(upper(trim(`net_manager`)), '') as net_manager,
        nullif(upper(trim(`purchase_area`)), '') as purchase_area,
        nullif(upper(trim(`street`)), '') as street,
        nullif(upper(trim(`zipcode`)), '') as zipcode,
        nullif(upper(trim(`city`)), '') as city,
        try_cast(`delivery_perc` as decimal(10,4)) as delivery_pct,
        try_cast(`perc_of_active_connections` as decimal(10,4)) as pct_of_active_connections,
        try_cast(`type_conn_perc` as decimal(10,4)) as type_conn_pct,
        nullif(trim(`type_of_connection`), '') as type_of_connection,
        try_cast(`annual_consume` as int) as annual_consume,
        try_cast(`annual_consume_lowtarif_perc` as decimal(10,4)) as annual_consume_lowtarif_pct,
        try_cast(`smartmeter_perc` as decimal(10,4)) as smartmeter_pct,
        try_cast(`num_connections` as int) as num_connection,
        try_cast(year as int) as year

        from source_data
)

select *
from rename_and_casted
limit 10