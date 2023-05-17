{% macro unnest(array, alias = "") -%}
  {{ return (adapter.dispatch('unnest', 'fhir_dbt_analytics') (array, alias)) }}
{%- endmacro %}


{% macro default__unnest(array, alias) -%}
  UNNEST({{ array }}){% if alias|length > 0 %} {{ alias }}{% endif %}
{%- endmacro -%}


{# Select the field as `ac`: this "cheeky select" trick allows us to write the unnest as one
    statement. #}
{% macro spark__unnest(array, alias) -%}
  SELECT EXPLODE(ac){% if alias|length > 0 %} AS {{ alias }}{% endif %} FROM (SELECT {{ array }} AS ac)
{%- endmacro %}
