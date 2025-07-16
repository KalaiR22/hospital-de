  {{config(
        schema = 'intermediate',
        materialized='incremental',
        unique_key = 'doctor_id',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags = 'doctors'
    )
}}

select {{ dbt_utils.star(from=ref('t_doctors') ) }}
from {{ ref('t_doctors') }}
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where last_updated_at > (select max(last_updated_at) from {{ this }}) 
{% endif %}

