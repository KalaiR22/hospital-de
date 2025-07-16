{% macro generate_yaml_for(model_names) %}
  {% if model_names is string or model_names is not iterable %}
    {% set model_names = [model_names] %}
  {% endif %}

  {% for model_name in model_names %}
    {{ dbt_utils.generate_model_yaml(model_name=model_name) }}
    
  {% endfor %}
{% endmacro %}
