
{{
    config(
        schema = 'transformation'
    )
}}

select 
 concat('T', cast(row_number() over (order by treatment_id) as varchar)) as treatment_key,
  treatment_id,
  appointment_id,
  coalesce(treatment_type,'unknown') as treatment_type ,
  coalesce(description,'unknown') as description,
  coalesce(cost,0) as cost,
  to_date(treatment_date) as treatment_date,
  current_timestamp() as updated_at
from {{ ref('stg_treatments') }}