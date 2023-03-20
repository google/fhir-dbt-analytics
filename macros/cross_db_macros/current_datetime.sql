{% macro current_datetime() -%}
  {{ return(adapter.dispatch('current_datetime', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}


{% macro default__current_datetime() -%}
  CURRENT_DATETIME()
{%- endmacro %}


{% macro spark__current_datetime() -%}
  CURRENT_TIMESTAMP()
{%- endmacro %}