{% macro date(expression, timezone=None) -%}
  {{ return(adapter.dispatch('date', 'fhir_dbt_analytics') (expression, timezone)) }}
{%- endmacro %}


{% macro default__date(expression, timezone) -%}
  DATE({{ expression }}, {{ timezone }})
{%- endmacro %}


{% macro spark__date(expression, timezone) -%}
  DATE({{ expression }})
{%- endmacro %}