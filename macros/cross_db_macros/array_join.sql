{% macro array_join(array, delimiter) -%}
  {{ return(adapter.dispatch('array_join', 'fhir_dbt_analytics') (array, delimiter)) }}
{%- endmacro %}


{% macro default__array_join(array, delimiter) -%}
  ARRAY_TO_STRING({{ array }}, "{{ delimiter }}")
{%- endmacro %}


{% macro spark__array_join(array, delimiter) -%}
  array_join({{ array }}, "{{ delimiter }}")
{%- endmacro %}