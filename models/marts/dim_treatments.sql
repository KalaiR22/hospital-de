{{
    config(
        materialized='table',
        schema = 'marts',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags = 'treatments'
    )
}}

select {{ dbt_utils.star(ref('int_treatment_scd'))}}  
from {{ ref('int_treatment_scd') }}
