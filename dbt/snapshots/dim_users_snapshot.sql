{% snapshot dim_users_snapshot %}

{{
    config(
      unique_key='userId',
      strategy='timestamp',
      updated_at='timestamp',
      invalidate_hard_deletes=True,
      target_schema=var('silver_schema', 'silver')
    )
}}

with user_attributes as (
    select
        userId,
        firstName,
        lastName,
        gender,
        level,
        registration_time,
        timestamp,
        -- Use the most recent values for each attribute when multiple records
        row_number() over (partition by userId order by timestamp desc) as rn
    from {{ ref('bronze_listen_events') }}
    where userId is not null
)

select
    userId,
    firstName,
    lastName,
    gender,
    level,
    registration_time,
    timestamp
from user_attributes
where rn = 1

{% endsnapshot %} 