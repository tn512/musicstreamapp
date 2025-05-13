{{ config(
    materialized='table'
) }}

select 
    artistName,
    count(*) as total_streams,
    count(distinct userId) as unique_users,
    count(distinct songKey) as unique_songs,
    count(distinct date_part) as active_days
from {{ ref('wide_fact') }}
group by 
    artistName 