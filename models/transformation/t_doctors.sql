{{
    config(
        schema = 'transformation',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags = 'doctors'
    )
}}

select 
   concat('D', cast(row_number() over (order by doctor_id) as varchar)) as doctor_key,
   doctor_id,
   coalesce(first_name,'unknown') as first_name,
   coalesce(last_name,'unknown') as last_name,
   coalesce(specialization,'unknown') as specialization,
   coalesce(phone_number,0000000000) as phone_number,
   coalesce(years_experience,0) as years_experience,
   coalesce(hospital_branch,'unknown') as hospital_branch,
   coalesce(email ,'unknown@hospital.com') as email,
from {{ ref('stg_doctors') }}