{% macro date_add_days(expression, days) -%}
  {{ return(adapter.dispatch('date_add_days', 'fhir_dbt_analytics') (expression, days)) }}
{%- endmacro %}


{% macro default__date_add_days(expression, days) -%}
  DATE_ADD({{ expression }}, INTERVAL {{ days }} DAY)
{%- endmacro %}


{% macro spark__date_add_days(expression, days) -%}
  DATE_ADD({{ expression }}, {{ days }})
{%- endmacro %}