{% macro uuid() -%}
  {{ return(adapter.dispatch('uuid', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}

{% macro default__uuid() -%}
  UUID()
{%- endmacro %}

{% macro bigquery__uuid() -%}
  GENERATE_UUID()
{%- endmacro %}