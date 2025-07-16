{{
    config(
        schema = 'intermediate',
        materialized='table',
        unique_key = 'doctor_id',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags='patients'
    )
}}

select patient_key,
       patient_id,
       first_name,
       last_name,
       gender,
       date_of_birth,
       contact_number,
       address,
       registration_date,
       insurance_provider,
       insurance_number,
       email,
       dbt_valid_from as effective_start_date,
       dbt_valid_to as effective_end_date,
       case
       when dbt_valid_to is null then 'Y'
       else 'N'
       end as is_current
from {{ ref('snapshot_patient') }}