{% macro failed_log(results) %}
  {% for result in results %}
    {% if result.status == 'failed' or result.status == 'error' %}
      UPDATE logs.dbt_model_control_table
      SET run_ended_at = CURRENT_TIMESTAMP,
          status = 'error'
      WHERE invocation_id = '{{ invocation_id }}';
    {% endif %}
  {% endfor %}
{% endmacro %}
