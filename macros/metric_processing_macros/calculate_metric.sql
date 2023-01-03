{%- macro calculate_metric(inner_sql, numerator=None, denominator=None) -%}

{# Check that all inputs are available. #}
{# We need to use namespace to update the variable in the `for` cycle. #}
{%- set ns = namespace(
    inputs_available = fhir_resource_exists(model_metadata('primary_resource'))) -%}
{%- if model_metadata('primary_fields') -%}
    {%- for primary_field in model_metadata('primary_fields') -%}
        {% set ns.inputs_available = ns.inputs_available and column_exists(primary_field) -%}
    {%- endfor -%}
{%- endif %}    

{%- if ns.inputs_available -%}
WITH
  A AS (
    {{ inner_sql }}
  )
{{ metric_output(numerator, denominator) }}
{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}
{%- endmacro -%}