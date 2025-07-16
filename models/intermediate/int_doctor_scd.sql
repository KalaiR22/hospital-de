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
            'email',
           'last_updated_at',  
            'updated_at'  
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


