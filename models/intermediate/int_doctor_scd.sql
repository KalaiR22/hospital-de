{{
    config(
        database = 'hospital',
        schema='intermediate',
        materialized='incremental',
        unique_key='doctor_id',
        pre_hook=["{{ start_log(invocation_id) }}"],
        post_hook=["{{ end_log(invocation_id) }}"],
        tags=['doctors']
    )
}}

with src as (
    select
        doctor_id,
        coalesce(first_name, 'unknown') as first_name,
        coalesce(last_name, 'unknown') as last_name,
        coalesce(specialization, 'unknown') as specialization,
        coalesce(phone_number, 0000000000) as phone_number,
        coalesce(years_experience, 0) as years_experience,
        coalesce(hospital_branch, 'unknown') as hospital_branch,
        coalesce(email, 'unknown@hospital.com') as email
    from {{ ref('stg_doctors') }}
),

existing as (
    select
        doctor_id,
        coalesce(first_name, 'unknown') as first_name,
        coalesce(last_name, 'unknown') as last_name,
        coalesce(specialization, 'unknown') as specialization,
        coalesce(phone_number, 0000000000) as phone_number,
        coalesce(years_experience, 0) as years_experience,
        coalesce(hospital_branch, 'unknown') as hospital_branch,
        coalesce(email, 'unknown@hospital.com') as email,
        created_at,
        last_updated_at
    from {{ this }}
),

final as (
    select
        src.*,

        -- created_at logic
        case
            when existing.doctor_id is null then current_timestamp
            else existing.created_at
        end as created_at,

        -- last_updated_at logic
        case
            when existing.doctor_id is null then current_timestamp
            when src.first_name <> existing.first_name
              or src.last_name <> existing.last_name
              or src.specialization <> existing.specialization
              or src.phone_number <> existing.phone_number
              or src.years_experience <> existing.years_experience
              or src.hospital_branch <> existing.hospital_branch
              or src.email <> existing.email
            then current_timestamp
            else existing.last_updated_at
        end as last_updated_at
    from src
    left join existing
    on src.doctor_id = existing.doctor_id
)

select * from final
