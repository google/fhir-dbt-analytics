{% macro unnest(array, alias = "unused") -%}
  {{ return (adapter.dispatch('unnest', 'fhir_dbt_analytics') (array, alias)) }}
{%- endmacro %}


{% macro default__unnest(array, alias) -%}
  UNNEST({{ array }}) {{ alias }}
{%- endmacro -%}


{# Select the field as `ac`: this "cheeky select" trick allows us to write the unnest as one
    statement. #}
{% macro spark__unnest(array, alias) -%}
  SELECT EXPLODE(ac) AS {{ alias }} FROM (SELECT {{ array }} AS ac)
{%- endmacro %}
