 
{{
    config(
        schema = 'transformation',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"]
    )
}}

 
 
 select
    concat(left(treatment_type,3), cast(row_number() over (order by treatment_type,DESCRIPTION

 ) as varchar)) as treatment_key,
        treatment_type,DESCRIPTION,
        concat(cast(min(cost) as varchar), '-', cast(max(cost) as varchar)) as cost_range,
         current_timestamp() as last_updated
    from {{ ref('stg_treatments') }}
    group by treatment_type,DESCRIPTION
