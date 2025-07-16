{{
    config(
        schema = 'transformation',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags='patients'
    )
}}

select 
   concat('P-', cast(row_number() over (order by patient_id) as varchar)) as patient_key,
  patient_id,
  coalesce(first_name,'unknown') as first_name,
  coalesce(last_name,'unknown') as last_name,
  coalesce(gender,'unknown') as gender,
  to_date(date_of_birth) as date_of_birth,
  coalesce(contact_number,0000000000) as contact_number,
  coalesce(address,'unknown') as address,
  to_date(registration_date) as registration_date,
  coalesce(insurance_provider,'unknown') as insurance_provider,
  coalesce(insurance_number,'unknown') as insurance_number,
  coalesce(email,'unknown@mail.com') as email,
from {{ ref('stg_patients') }}