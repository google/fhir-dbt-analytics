{% macro type_double() -%}
  {{ return(adapter.dispatch('type_double', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}

{% macro default__type_double() -%}
  DOUBLE
{%- endmacro %}

{% macro bigquery__type_double() -%}
  FLOAT64
{%- endmacro %}