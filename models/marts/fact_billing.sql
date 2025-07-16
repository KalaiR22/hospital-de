{{
    config(
        materialized='table',
        schema = 'marts',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags = 'billings'
    )
}}

select {{ dbt_utils.star(from=ref('int_billing') ,  except = ["bill_id","patient_id"])}}  
from {{ ref('int_billing') }}