{% macro safe_offset(array, index) -%}
  {{ return(adapter.dispatch('safe_offset', 'fhir_dbt_analytics') (array, index)) }}
{%- endmacro %}


{% macro default__safe_offset(array, index) -%}
  {{ array }}[SAFE_OFFSET({{ index }})]
{%- endmacro %}


{% macro spark__safe_offset(array, index) -%}
  element_at({{ array }}, {{ index + 1 }})
{%- endmacro %}