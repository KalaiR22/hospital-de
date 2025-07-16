{% macro end_log( invocation_id,status='success') %}
    UPDATE logs.dbt_model_control_table
    SET run_ended_at = CURRENT_TIMESTAMP,
        status = '{{ status }}'
    WHERE  invocation_id =  '{{invocation_id}}';
{% endmacro %}

