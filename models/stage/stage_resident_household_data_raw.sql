with source_data as (

    select *
    from {{ source('solar_panel', 'resident_household_data_raw') }}
),

rename_and_casted as(
    select 

        nullif(upper(trim(zipcode)), '') as zipcode,
        
        try_cast(`total_occupants` as int)
            as total_occupants,
        
        try_cast(`male_occupants` as int)
            as male_occupants,

        try_cast(`female_occupants` as int)
            as female_occupants,

        try_cast(`occupants_aged_below_15yrs` as int)
            as occupants_aged_below_15yrs,

        try_cast(`occupants_aged_15_to_25_yrs` as int)
            as occupants_aged_15_to_25yrs,

        try_cast(`occupants_aged_25_to_45_yrs` as int)
            as occupants_aged_25_to_45_yrs,

        try_cast(`occupants_aged_45_to_65_yrs` as int)
            as occupants_aged_45_to_65_yrs,
            
        try_cast(`occupants_aged_above_65yrs` as int)
            as occupants_aged_above_65yrs,

        try_cast(`total_number_of_households` as int)
            as total_number_of_households,

        try_cast(`number_of_one-persons_households` as int)
            as number_of_one_persons_households,

        try_cast(`number_of_multi-person_nochildren_households` as int)
            as number_of_multi_person_nochildren_households,

        try_cast(`number_of_single-parent_households` as int)
            as number_of_single_parent_households,

        try_cast(`number_of_two-parent_households` as int)
            as number_of_two_parent_households,

        try_cast(`total_number_of_homes` as int)
            as total_number_of_homes,

        try_cast(`number_of_homes_build_year_pre_1945` as int)
            as number_of_homes_build_year_pre_1945,

        try_cast(`number_of_homes_build_year_1945_to_1965` as int)
            as number_of_homes_build_year_1945_to_1965,

        try_cast(`number_of_homes_build_year_1965_to_1975` as int)
            as number_of_homes_build_year_1965_to_1975,

        try_cast(`number_of_homes_build_year_1975_to_1985` as int)
            as number_of_homes_build_year_1975_to_1985,

        try_cast(`number_of_homes_build_year_1985_to_1995` as int)
            as number_of_homes_build_year_1985_to_1995,

        try_cast(`number_of_homes_build_year_1995_to_2005` as int)
            as number_of_homes_build_year_1995_to_2005,

        try_cast(`number_of_homes_build_year_2005_to_2015` as int)
            as number_of_homes_build_year_2005_to_2015,

        try_cast(`number_of_homes_build_year_post_2015` as int)
            as number_of_homes_build_year_post_2015,

        try_cast(`%_owner-occupied_property` as decimal(10,4))
            as pct_owner_occupied_property,

        try_cast(`%_rental_property` as decimal(10,4))
            as pct_rental_property,

        try_cast(`number_of_not_occupied_property` as int)
            as number_of_not_occupied_property,

        try_cast(`average_woz_household_value_in_kEUR` as int)
            as average_woz_household_value_in_kEUR

        from source_data
)

select *
from rename_and_casted