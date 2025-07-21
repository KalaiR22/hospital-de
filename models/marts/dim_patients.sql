{{
    config(
        materialized='table',
        schema = 'marts',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags='patients'
    )
}}

select {{ dbt_utils.star(from=ref('int_patients') , except = ["patient_id"] )}}  
from {{ ref('int_patients') }}