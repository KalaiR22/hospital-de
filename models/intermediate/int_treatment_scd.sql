
{{
    config(
        schema='intermediate',
        materialized='incremental',
        unique_key='treatment_key',
        merge_update_columns=[
            'treatment_type',
            'description',
            'cost_range',
            'last_updated',  
            'updated_at'        
        ],
         pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags=['treatments']
    )
}}

select
    treatment_key,
    treatment_type,
    description,
    cost_range,
    last_updated,
    current_timestamp() as updated_at
from {{ ref('t_treatments_type') }}


