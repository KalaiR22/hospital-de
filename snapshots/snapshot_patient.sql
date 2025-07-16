{% snapshot snapshot_patient %}
    {{
        config(
            target_schema='snapshot',
            unique_key='patient_id',
            strategy='check',
            check_cols = ['date_of_birth','contact_number','insurance_provider'],
            invalidate_hard_deletes=False,
            pre_hook=["{{start_log( invocation_id) }}"],
            post_hook=["{{ end_log( invocation_id) }}"],
            tags='patients'
        )
    }}

    select * from {{ ref('t_patients') }}
 {% endsnapshot %}