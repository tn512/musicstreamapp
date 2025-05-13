{{ config(
    materialized='table',
    partition_by={
        "field": "date_part",
        "data_type": "date"
    }
) }}

select 
    date_part,
    city,
    state,
    count(*) as total_streams,
    count(distinct userId) as unique_users,
    count(distinct songKey) as unique_songs
from {{ ref('wide_fact') }}
group by 
    date_part,
    city,
    state 