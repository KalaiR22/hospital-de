{{
    config(
        schema = 'transformation',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags = 'billings'
    )
}}
select 
   concat('B-', cast(row_number() over (order by  bill_id) as varchar), substring(patient_id,2),substring(treatment_id,2)) as bill_key,
   bill_id,
   patient_id,
   concat('T', cast(row_number() over (order by treatment_id ) as varchar)) as treatment_key,
   treatment_id,
   to_date(bill_date) as bill_date,
   coalesce(amount, 000) as amount,
   coalesce(payment_method, 'unknown') as payment_method,
   coalesce(payment_status, 'unknown') as  payment_status
from {{ ref('stg_billing') }}