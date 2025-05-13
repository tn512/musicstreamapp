{{ config(
    materialized='incremental',
    unique_key='bronze_id',
    incremental_strategy='merge',
    file_format='delta'
) }}

with source_data as (
    select 
        ts,
        sessionId,
        userId,
        auth,
        level,
        itemInSession,
        city,
        zip,
        state,
        userAgent,
        lon as longitude,
        lat as latitude,
        firstName,
        lastName,
        gender,
        registration,
        prevLevel,
        method,
        status,
        statusChangeType,
        kafka_timestamp,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        ingestion_timestamp
    from {{ source('music_streaming', 'status_change_events') }}
    
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)

select
    md5(concat(cast(ts as string), sessionId, userId, prevLevel, level)) as bronze_id,
    userId,
    from_unixtime(ts/1000) as timestamp,
    sessionId,
    prevLevel,
    level,
    method,
    status,
    statusChangeType,
    firstName,
    lastName,
    gender,
    city,
    state,
    zip,
    latitude,
    longitude,
    from_unixtime(registration/1000) as registration_time,
    kafka_timestamp,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    ingestion_timestamp,
    current_date() as ingestion_date
from source_data
where ts is not null
  and userId is not null 