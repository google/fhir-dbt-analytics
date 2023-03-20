{% macro is_spark() -%}
  {{ return(adapter.dispatch('is_spark', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}


{% macro default__is_spark() -%}
  {{ return (False) }}
{%- endmacro %}


{% macro spark__is_spark() -%}
  {{ return (True) }}
{%- endmacro %}