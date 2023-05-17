{% macro reference_resolves(
  reference_column,
  reference_resource
) -%}

{%- set reference_paths = get_reference_paths(reference_column, reference_resource) -%}
{%- set direct_reference = reference_paths['direct_reference'] -%}
{%- set direct_reference_path = reference_paths['direct_reference_path'] -%}
{%- set indirect_reference_path = reference_paths['indirect_reference_path'] -%}
{%- set reference_column_is_array = reference_paths['reference_column_is_array']-%}

{%- if reference_column_is_array -%}

  {%- if column_exists(direct_reference_path) -%}
    (SELECT SIGN(COUNT(*)) FROM {{ spark_parenthesis(unnest(reference_column, "RC")) }} JOIN {{ref(reference_resource)}} AS RR ON RC.{{direct_reference}} = RR.id)
  {%- elif column_exists(indirect_reference_path) -%}
    (SELECT SIGN(COUNT(*)) FROM {{ spark_parenthesis(unnest(reference_column, "RC")) }} JOIN {{ref(reference_resource)}} AS RR ON RC.reference = RR.id AND RC.type = '{{reference_resource}}')
  {%- else -%}
    0
  {%- endif -%}

{%- else -%}

  {{ _reference_resolves(direct_reference_path, indirect_reference_path, reference_resource, reference_column) }}

{%- endif -%}

{%- endmacro -%}


{# Spark can't process IN (SELECT) while BigQuery can't process corelated query. So we need
   different definitions. #}
{% macro _reference_resolves(direct_reference_path, indirect_reference_path, reference_resource, reference_column) -%}
  {{ return(adapter.dispatch('_reference_resolves', 'fhir_dbt_analytics')
       (direct_reference_path, indirect_reference_path, reference_resource, reference_column)) }}
{%- endmacro %}


{% macro default___reference_resolves(direct_reference_path, indirect_reference_path, reference_resource, reference_column) -%}
  {%- if column_exists(direct_reference_path) -%}
    IF({{direct_reference_path}} IN (SELECT id FROM {{ ref(reference_resource) }}), 1, 0)
  {%- elif column_exists(indirect_reference_path) -%}
    IF({{reference_column}}.type = '{{reference_resource}}' AND {{indirect_reference_path}} IN (SELECT id FROM {{ref(reference_resource)}}), 1, 0)
  {%- else -%}
    0
  {%- endif -%}
{%- endmacro %}


{% macro spark___reference_resolves(direct_reference_path, indirect_reference_path, reference_resource, reference_column) -%}
  {%- if column_exists(direct_reference_path) -%}
    (SELECT SIGN(COUNT(*)) FROM {{ ref(reference_resource) }} WHERE id = {{direct_reference_path}})
  {%- elif column_exists(indirect_reference_path) -%}
    (SELECT SIGN(COUNT(*)) FROM {{ ref(reference_resource) }} WHERE id = {{indirect_reference_path}} AND {{reference_column}}.type = '{{reference_resource}}')
  {%- else -%}
    0
  {%- endif -%}
{%- endmacro %}
