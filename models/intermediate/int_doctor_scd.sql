{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='doctor_id',
        merge_update_columns=[
            'first_name',
            'last_name',
            'specialization',
            'phone_number',
            'years_experience',
            'hospital_branch',
            'email'
        ],
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags=['doctors']
    )
}}

select 
    doctor_key,
    doctor_id,
    first_name,
    last_name,
    specialization,
    phone_number,
    years_experience,
    hospital_branch,
    email,
    last_updated_at,  
    current_timestamp() as updated_at  
from {{ ref('t_doctors') }}

{% if is_incremental() %}
    where last_updated_at > (select max(last_updated_at) from {{ this }})
{% endif %}
