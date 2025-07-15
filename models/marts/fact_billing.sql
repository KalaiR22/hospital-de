{{
    config(
        materialized='table',
        schema = 'marts',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"]
    )
}}

select {{ dbt_utils.star(from=ref('t_billing') ,  except = ["bill_id","patient_id","treatment_id"])}}  
from {{ ref('t_billing') }}