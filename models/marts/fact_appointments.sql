{{
    config(
        materialized='table',
        schema = 'marts',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
         tags= 'appointments'
    )
}}

select {{ dbt_utils.star(from=ref('int_appointments') ,  except = ["appointment_id","patient_id","doctor_id"])}}  
from {{ ref('int_appointments') }}