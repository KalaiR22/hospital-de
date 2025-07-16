{{
    config(
        materialized='table',
        schema = 'staging',
        pre_hook=["{{start_log( invocation_id) }}"],
        post_hook=["{{ end_log( invocation_id) }}"],
        tags = 'billings'
    )
}}

select * from {{ source('hospital_data', 'billing') }}