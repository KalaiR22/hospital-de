{{
    config(
        materialized='table',
        schema = 'marts',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"]
    )
}}

select {{ dbt_utils.star(from=ref('int_patients') )}}  
from {{ ref('int_patients') }}