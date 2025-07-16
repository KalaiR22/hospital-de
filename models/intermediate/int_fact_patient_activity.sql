{{
    config(
        schema = 'intermediate',
        materialized = 'table',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
         tags = 'patient_activity'
    )
}}




with appointment_fct as (
    select
      patient_id,
      count(appointment_id) as total_appointments,
    count(case when status = 'No-show' then 1  end) as no_show_count,
    min(appointment_date) as first_appointment_date,
    max(appointment_date) as last_appointment_date
    from {{ ref('t_appointments') }}
    group by patient_id
    order by patient_id
),
billing_fct as (
    select patient_id,
    sum(amount) as total_billed,
    sum(case
    when payment_status = 'Paid' then amount
    end ) as total_paid
    from {{ ref('t_billing') }}
    group by patient_id
)
,patient_ids as (
    select patient_id from {{ ref('int_patients') }}
)

, final_cte as( select p.patient_id,
     coalesce(total_appointments,0) as total_appointments,
     coalesce(total_billed,0) as total_billed,
     coalesce(total_paid,0) as total_paid,
     coalesce(no_show_count,0) as no_show_count,
    coalesce(cast(first_appointment_date as varchar),  '00-00-0000') as first_appointment_date ,
    coalesce(cast(last_appointment_date as varchar), '00-00-0000') as last_appointment_date
from patient_ids p
left join billing_fct b
on p.patient_id= b.patient_id
left join appointment_fct a
on p.patient_id = a.patient_id
order by patient_id
)
select concat('P-',substring(patient_id,2)) as patient_key, 
       total_appointments,
       total_billed,
       total_paid,
       no_show_count,
       first_appointment_date ,
       last_appointment_date
 from  final_cte 
