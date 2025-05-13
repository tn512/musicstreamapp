{{ config(
    materialized='table',
    tags=['dimension']
) }}

with user_history as (
    select
        userId,
        firstName,
        lastName,
        gender,
        level,
        registration_time,
        timestamp,
        dbt_valid_from as row_activation_date,
        coalesce(dbt_valid_to, cast('9999-12-31' as timestamp)) as row_expiration_date,
        case 
            when dbt_valid_to is null then true
            else false
        end as current_row,
        md5(concat(
            cast(userId as string),
            coalesce(level, 'unknown'),
            coalesce(cast(dbt_valid_from as string), '')
        )) as userKey
    from {{ ref('dim_users_snapshot') }}
)

select
    userKey,
    userId,
    coalesce(firstName, 'Unknown') as firstName,
    coalesce(lastName, 'Unknown') as lastName,
    coalesce(gender, 'Unknown') as gender,
    level,
    registration_time,
    cast(row_activation_date as date) as row_activation_date,
    cast(row_expiration_date as date) as row_expiration_date,
    current_row 