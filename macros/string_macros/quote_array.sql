{% macro quote_array(x) %}
  {% set result=[] %}
  {% for i in x %}
    {{ result.append("'" ~ dbt.escape_single_quotes(i) ~ "'") }}
  {% endfor %}
  {% do return (result) %}
{% endmacro %}