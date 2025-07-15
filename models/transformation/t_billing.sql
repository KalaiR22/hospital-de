{{
    config(
        schema = 'transformation',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"]
    )
}}
select 
   concat('B-', cast(row_number() over (order by  bill_id) as varchar), substring(patient_id,2),substring(treatment_id,2)) as bill_key,
   bill_id,
   concat(left(patient_id,1) ,'-', substring(patient_id,2)) as patient_key,
   patient_id,
   concat(left(treatment_id,1) ,'-', substring(treatment_id,2)) as treatment_key,
   treatment_id,
   to_date(bill_date) as bill_date,
   coalesce(amount, 000) as amount,
   coalesce(payment_method, 'unknown') as payment_method,
   coalesce(payment_status, 'unknown') as  payment_status
from {{ ref('stg_billing') }}