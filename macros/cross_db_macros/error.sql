{% macro error(expression) -%}
  {{ return(adapter.dispatch('error', 'fhir_dbt_analytics') (expression)) }}
{%- endmacro %}


{% macro default__error(expression) -%}
  ERROR("{{ expression }}")
{%- endmacro %}


{% macro spark__error(expression) -%}
  raise_error("{{ expression }}")
{%- endmacro %}