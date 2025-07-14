{{
    config(
        schema = 'transformation'
    )
}}

select
   concat('A', cast(row_number() over (order by appointment_id) as varchar)) as appointment_key, 
   appointment_id,
   concat(left(patient_id,1) ,'-', substring(patient_id,2)) as patient_key,
   patient_id,
   concat(left(doctor_id,1) ,'-', substring(doctor_id,2)) as doctor_key,
   doctor_id,
   to_date(appointment_date) as appointment_date, 
   to_time(appointment_time) as appointment_time,
    coalesce(reason_for_visit, 'unknown') as reason_for_visit , 
   coalesce(status, 'unknown') as status

from {{ ref('stg_appointments') }}
