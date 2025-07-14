with appointment_fct as (
    select
      patient_id,
      count(appointment_id) as total_appointments,
      sum(amount) as total_billed,
      case
    when payment_status = 'paid' then sum(amount) as total_paid
    from {{ ref('t_appointments') }}
)