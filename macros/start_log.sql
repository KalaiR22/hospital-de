{% macro start_log( invocation_id) %}
    INSERT INTO logs.dbt_model_control_table ( invocation_id,model_name, run_started_at, status)
    VALUES ('{{invocation_id}}','{{ this.name }}', CURRENT_TIMESTAMP, 'running');
{% endmacro %}
