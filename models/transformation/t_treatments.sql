
{{
    config(
        schema = 'transformation',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"]
    )
}}



select 
 concat('T', cast(row_number() over (order by treatment_id) as varchar)) as treatment_key,
  c.treatment_key as treatment_id ,
  appointment_id,
  coalesce(t.treatment_type,'unknown') as treatment_type ,
  coalesce(t.description,'unknown') as description,
  coalesce(cost,0) as cost,
  cost_range,
  to_date(treatment_date) as treatment_date,
  current_timestamp() as last_updated
from {{ ref('stg_treatments') }} t
join {{ ref('t_treatments_type') }} c
on t.treatment_type = c.treatment_type