{% macro type_long() -%}
  {{ return(adapter.dispatch('type_long', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}

{% macro default__type_long() -%}
  LONG
{%- endmacro %}

{% macro bigquery__type_long() -%}
  INT64
{%- endmacro %}