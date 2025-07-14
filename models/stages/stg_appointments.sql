{{
    config(
        materialized='table',
        schema = 'staging'
    )
}}

select * from {{ source('hospital_data', 'appointments') }}