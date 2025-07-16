
{{
    config(
        schema = 'transformation',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"]
    )
}}

with treatment_cost_range as (
    select
        treatment_type,
        concat(cast(min(cost) as varchar), '-', cast(max(cost) as varchar)) as cost_range
    from {{ ref('stg_treatments') }}
    group by treatment_type
)

select 
 concat('T', cast(row_number() over (order by treatment_id) as varchar)) as treatment_key,
  concat(left(t.treatment_type,3),substring(treatment_id,2)) as treatment_id ,
  appointment_id,
  coalesce(t.treatment_type,'unknown') as treatment_type ,
  coalesce(description,'unknown') as description,
  coalesce(cost,0) as cost,
  cost_range,
  to_date(treatment_date) as treatment_date,
  current_timestamp() as last_updated
from {{ ref('stg_treatments') }} t
join treatment_cost_range c
on t.treatment_type = c.treatment_type