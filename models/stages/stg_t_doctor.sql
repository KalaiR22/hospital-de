{{
    config(
        materialized='table',
        schema = 'staging',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags = 'doctors'
    )
}}

{% set dim_doctor_relation = adapter.get_relation(
    database='hospital',
    schema='marts',
    identifier='dim_doctor'
) %}

{% if dim_doctor_relation is not none %}
    select * from {{ source('t_data', 'dim_doctor') }}
{% else %}
    select null as doctor_key, null as doctor_id, null as first_name, null as last_name, null as specialization, 
           null as phone_number, null as years_experience, null as hospital_branch, 
           null as email, null as last_updated_at
    where false
{% endif %}
