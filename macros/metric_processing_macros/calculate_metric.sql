{%- macro calculate_metric(inner_sql, numerator=None, denominator=None, measure=None) -%}

{% if _are_inputs_available() %}
WITH
  A AS (
    {{ inner_sql }}
  )
{{ metric_output(numerator, denominator, measure) }}
{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}
{%- endmacro -%}


{# Check that all inputs are available. #}
{% macro _are_inputs_available() %}
  {% if not fhir_resource_exists(model_metadata('primary_resource')) %}
    {{ _print_why("primary resource " ~ model_metadata('primary_resource')) }}
    {% do return(False) %}
  {% endif %}
  {% for secondary_resource in model_metadata('secondary_resources', value_if_missing=[])
      if not fhir_resource_exists(secondary_resource) %}
    {{ _print_why("secondary resource " ~ secondary_resource) }}
    {% do return(False) %}
  {% endfor %}
  {% for primary_field in model_metadata('primary_fields', value_if_missing=[])
      if not column_exists(primary_field) %}
    {{ _print_why("primary field " ~ primary_field) }}
    {% do return(False) %}
  {% endfor %}

  {% do return(True) %}
{% endmacro %}


{# Print why is the metric empty, which input was not available. #}
{% macro _print_why(what_is_missing) %}
  {% if var('print_why_metric_empty') and execute %}
    {{ print(this.name ~ " empty because " ~ what_is_missing ~ " is not available") }}
  {% endif %}
{% endmacro %}

