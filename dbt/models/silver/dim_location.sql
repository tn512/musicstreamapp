{{ config(
    materialized='table',
    tags=['dimension']
) }}

with locations as (
    select
        city,
        state as stateCode,
        zip,
        latitude,
        longitude
    from {{ ref('bronze_listen_events') }}
    where city is not null
    and state is not null
    
    union distinct
    
    select
        city,
        state as stateCode,
        zip,
        latitude,
        longitude
    from {{ ref('bronze_page_view_events') }}
    where city is not null
    and state is not null
),

-- Deduplicate locations
unique_locations as (
    select distinct
        city,
        stateCode,
        zip,
        latitude,
        longitude
    from locations
)

select
    md5(concat(
        coalesce(city, 'unknown'),
        coalesce(stateCode, 'NA'),
        coalesce(zip, 'unknown')
    )) as locationKey,
    city,
    stateCode,
    -- Join with state_codes (seed data) to get full state names
    coalesce(s.stateName, 'Unknown') as stateName,
    zip,
    latitude,
    longitude
from unique_locations l
left join {{ ref('state_codes') }} s
    on l.stateCode = s.stateCode 