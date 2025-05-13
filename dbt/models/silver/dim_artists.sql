{{ config(
    materialized='table',
    tags=['dimension']
) }}

with artist_data as (
    select
        artist_id as artistId,
        artist_latitude as latitude,
        artist_longitude as longitude,
        artist_location as location,
        regexp_replace(regexp_replace(artist_name, '"', ''), '\\\\', '') as name
    from {{ source('music_streaming', 'songs') }}
    where artist_name is not null
),

-- Group by artist name to deduplicate
artist_grouped as (
    select
        name,
        max(artistId) as artistId,
        max(latitude) as latitude,
        max(longitude) as longitude,
        max(location) as location
    from artist_data
    group by name
)

select
    md5(artistId) as artistKey,
    artistId,
    name,
    location,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude
from artist_grouped 