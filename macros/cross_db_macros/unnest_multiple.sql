{% macro unnest_multiple(arrays) -%}
  {{ return (adapter.dispatch('unnest_multiple', 'fhir_dbt_analytics') (arrays)) }}
{%- endmacro %}


{% macro array_config(field, unnested_alias = 'unused') %}
  {{ return ({ "field": field, "unnested_alias": unnested_alias}) }}
{% endmacro %}


{% macro default__unnest_multiple(arrays) -%}
  {% for array in arrays -%}
    UNNEST({{ array.field }}) {{ array.unnested_alias }}
    {%- if not loop.last -%}, {% endif -%}
  {%- endfor -%}
{%- endmacro -%}


{% macro spark__unnest_multiple(arrays) -%}
  {# Select the field as `ac`: this "cheeky select" trick allows us to write the unnest as one
      statement. #}
  {%- set array0 = arrays[0] -%}
    SELECT * FROM (SELECT EXPLODE(ac) AS {{ array0.unnested_alias }} FROM (SELECT {{ array0.field }} AS ac))
  {%- for array in arrays[1:] %}
    LATERAL VIEW OUTER explode ({{ array.field }}) AS {{ array.unnested_alias }}
  {%- endfor -%}
{%- endmacro %}
