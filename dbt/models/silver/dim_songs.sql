{{ config(
    materialized='table',
    tags=['dimension']
) }}

select
    md5(cast(song_id as string)) as songKey,
    song_id as songId,
    title,
    artist_name as artistName,
    cast(duration as double) as duration,
    cast(key as int) as key,
    cast(key_confidence as double) as keyConfidence,
    cast(loudness as double) as loudness,
    cast(song_hotttnesss as double) as songHotness,
    cast(tempo as double) as tempo,
    cast(year as int) as year
from {{ source('music_streaming', 'songs') }}
where title is not null
and artist_name is not null 