{%- macro calculate_metric(inner_sql, numerator=None, denominator=None) -%}

{% if _are_inputs_available() %}
WITH
  A AS (
    {{ inner_sql }}
  )
{{ metric_output(numerator, denominator) }}
{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}
{%- endmacro -%}


{# Check that all inputs are available. #}
{%- macro _are_inputs_available() %}
    {% if not fhir_resource_exists(model_metadata('primary_resource')) %}
        {% do return(False) %}
    {% endif %}
    {% for primary_field in model_metadata('primary_fields', value_if_missing=[])
        if not column_exists(primary_field) %}
        {% do return(False) %}
    {% endfor %}

    {% do return(True) %}
{% endmacro -%}

