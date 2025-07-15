{{
    config(
        schema = 'intermediate',
        materialized='incremental',
        unique_key = 'treatment_id',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"]
    )
}}

select {{ dbt_utils.star(from=ref('t_treatments') ,  except = ["appointment_id"])}}
from {{ ref('t_treatments') }}
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where last_updated > (select max(last_updated) from {{ this }}) 
{% endif %}