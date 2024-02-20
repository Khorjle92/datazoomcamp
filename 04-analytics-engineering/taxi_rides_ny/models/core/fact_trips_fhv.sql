{{
    config(
        materialized='table'
    )
}}

with fhv2019 as (
    select *, 
        
    from {{ ref('stg_staging__fhv2019') }}
    WHERE extract(year from pickup_datetime) =2019
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv2019.dispatching_base_num, 
    fhv2019.pickup_datetime, 
    fhv2019.dropoff_datetime,
    fhv2019.pulocationid, 
    fhv2019.dolocationid, 
    fhv2019.sr_flag,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    
from fhv2019
inner join dim_zones as pickup_zone
on fhv2019.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv2019.dolocationid = dropoff_zone.locationid