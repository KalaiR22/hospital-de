{{
    config(
        schema = 'intermediate',
        materialized='table',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags= 'appointments'
    )
}}

select 
    appointment_key,
    appointment_id,
    d.doctor_key,
    p.patient_key,
    a.patient_id,
    a.doctor_id,
    appointment_date,
    appointment_time,
    reason_for_visit,
    status 
from {{ ref('t_appointments') }} a
join {{ ref('t_doctors') }} d
    on a.doctor_id = d.doctor_id
join {{ ref('t_patients') }} p
    on a.patient_id = p.patient_id
