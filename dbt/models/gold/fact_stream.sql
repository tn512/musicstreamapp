{{ config(
    materialized='incremental',
    unique_key=['userKey', 'songKey', 'artistKey', 'dateKey', 'timestamp'],
    partition_by={
        "field": "date_part",
        "data_type": "date"
    },
    file_format='delta'
) }}

with listen_events as (
    select
        userId,
        song,
        timestamp,
        city,
        state,
        latitude,
        longitude,
        regexp_replace(regexp_replace(artist, '"', ''), '\\\\', '') as artist_clean
    from {{ ref('bronze_listen_events') }}
    where userId is not null
    
    {% if is_incremental() %}
    and timestamp > (select max(timestamp) from {{ this }})
    {% endif %}
)

select
    u.userKey,
    a.artistKey,
    s.songKey,
    d.dateKey,
    l.locationKey,
    e.timestamp,
    cast(date(e.timestamp) as date) as date_part
from listen_events e
-- Join with dim_users (SCD Type 2)
left join {{ ref('dim_users') }} u
    on e.userId = u.userId
    and date(e.timestamp) >= u.row_activation_date
    and date(e.timestamp) < u.row_expiration_date
-- Join with dim_artists
left join {{ ref('dim_artists') }} a
    on e.artist_clean = a.name
-- Join with dim_songs
left join {{ ref('dim_songs') }} s
    on e.song = s.title
    and e.artist_clean = s.artistName
-- Join with dim_location
left join {{ ref('dim_location') }} l
    on e.city = l.city
    and e.state = l.stateCode
    and e.latitude = l.latitude
    and e.longitude = l.longitude
-- Join with dim_datetime
left join {{ ref('dim_datetime') }} d
    on date_trunc('hour', e.timestamp) = d.datetime 