{{
    config(
        materialized='table',
        schema = 'marts',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags = 'doctors'
    )
}}

select {{ dbt_utils.star(from=ref('int_doctor_scd'))}}  
from {{ ref('int_doctor_scd') }}
order by doctor_id