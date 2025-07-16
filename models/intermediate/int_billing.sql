{{
    config(
        schema = 'intermediate',
        materialized='table',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags = 'billings'
    )
}}

select bill_key,
    bill_id,
    p.patient_key,
    b.patient_id,
    t.treatment_id as treatment_key,
    bill_date,
    amount,
    payment_method,
    payment_status,
    appointment_id,
    treatment_type,
    description,
    cost,
    cost_range,
    treatment_date,
    last_updated from {{ ref('t_billing') }} b
join {{ ref('t_treatments') }} t
on b.treatment_key = t.treatment_key
join {{ ref('t_patients') }} p 
on b.patient_id = p.patient_id