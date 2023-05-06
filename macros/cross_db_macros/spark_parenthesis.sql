{% macro spark_parenthesis(s) -%}
  {{ return (adapter.dispatch('spark_parenthesis', 'fhir_dbt_analytics') (s)) }}
{%- endmacro %}


{% macro default__spark_parenthesis(s) -%}
  {{ s }}
{%- endmacro -%}


{% macro spark__spark_parenthesis(s) -%}
  ({{ s }})
{%- endmacro %}
