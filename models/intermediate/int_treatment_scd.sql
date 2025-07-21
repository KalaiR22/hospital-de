{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='treatment_key',
        pre_hook=["{{ start_log(invocation_id) }}"],
        post_hook=["{{ end_log(invocation_id) }}"],
        tags=['treatments']
    )
}}

with src as (
    select
        treatment_key,
        coalesce(treatment_type, 'unknown') as treatment_type,
        cost_range,
        coalesce(description, 'no description') as description
    from {{ ref('t_treatments_type') }}
),

existing as (
    select
        treatment_key,
        coalesce(treatment_type, 'unknown') as treatment_type,
        cost_range,
        coalesce(description, 'no description') as description,
        created_at,
        last_updated
    from {{ this }}
),

final as (
    select
        src.*,


        case
            when existing.treatment_key is null then current_timestamp
            else existing.created_at
        end as created_at,


        case
            when existing.treatment_key is null then current_timestamp
            when  src.treatment_type <> existing.treatment_type
              or src.cost_range <> existing.cost_range
              or src.description <> existing.description
            then current_timestamp
            else existing.last_updated
        end as last_updated

    from src
    left join existing
    on src.treatment_key = existing.treatment_key
)

select * from final



