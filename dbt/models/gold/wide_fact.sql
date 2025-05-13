{{ config(
    materialized='view'
) }}

select 
    f.userKey,
    f.artistKey,
    f.songKey,
    f.dateKey,
    f.locationKey,
    f.timestamp,
    u.firstName,
    u.lastName,
    u.gender,
    u.level,
    u.userId,
    s.duration as songDuration,
    s.title as songName,
    l.city,
    l.stateCode as state,
    l.latitude,
    l.longitude,
    d.datetime as dateHour,
    d.dayOfMonth,
    d.dayOfWeek,
    a.name as artistName
from {{ ref('fact_stream') }} f
inner join {{ ref('dim_users') }} u on f.userKey = u.userKey
inner join {{ ref('dim_songs') }} s on f.songKey = s.songKey
inner join {{ ref('dim_artists') }} a on f.artistKey = a.artistKey
inner join {{ ref('dim_location') }} l on f.locationKey = l.locationKey
inner join {{ ref('dim_datetime') }} d on f.dateKey = d.dateKey 