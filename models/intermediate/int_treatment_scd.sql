{{
    config(
        schema = 'intermediate',
        materialized='incremental',
        unique_key = 'treatment_key',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"]
    )
}}

select {{ dbt_utils.star(from=ref('t_treatments_type') )}}
from {{ ref('t_treatments_type') }}
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where last_updated > (select max(last_updated) from {{ this }}) 
{% endif %}