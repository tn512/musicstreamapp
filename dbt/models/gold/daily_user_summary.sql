{{ config(
    materialized='table',
    partition_by={
        "field": "date_part",
        "data_type": "date"
    }
) }}

select 
    date_part,
    userId,
    firstName,
    lastName,
    level,
    count(*) as total_streams,
    count(distinct songKey) as unique_songs,
    count(distinct artistKey) as unique_artists
from {{ ref('wide_fact') }}
group by 
    date_part,
    userId,
    firstName,
    lastName,
    level 