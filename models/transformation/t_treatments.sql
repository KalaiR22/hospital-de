
{{
    config(
        schema = 'transformation',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"]
    )
}}

select 
 concat('T', cast(row_number() over (order by treatment_id) as varchar)) as treatment_key,
  concat(left(treatment_type,3),substring(treatment_id,2)) as treatment_id ,
  appointment_id,
  coalesce(treatment_type,'unknown') as treatment_type ,
  coalesce(description,'unknown') as description,
  coalesce(cost,0) as cost,
  to_date(treatment_date) as treatment_date,
  current_timestamp() as updated_at
from {{ ref('stg_treatments') }}