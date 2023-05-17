{% macro has_reference_value(
  reference_column,
  reference_resource
) %}

{%- set reference_paths = get_reference_paths(reference_column, reference_resource) -%}
{%- set direct_reference = reference_paths['direct_reference'] -%}
{%- set direct_reference_path = reference_paths['direct_reference_path'] -%}
{%- set indirect_reference_path = reference_paths['indirect_reference_path'] -%}
{%- set reference_column_is_array = reference_paths['reference_column_is_array']-%}

{%- if reference_column_is_array -%}

  {%- if column_exists(direct_reference_path) -%}
    (SELECT SIGN(COUNT(*)) FROM {{ spark_parenthesis(unnest(reference_column, "RC")) }} WHERE {{has_value("RC."~direct_reference)}})
  {%- elif column_exists(indirect_reference_path) -%}
    (SELECT SIGN(COUNT(*)) FROM {{ spark_parenthesis(unnest(reference_column, "RC")) }} WHERE RC.type = '{{reference_resource}}' AND {{has_value('RC.reference')}})
  {%- else -%}
    0
  {%- endif -%}

{%- else -%}

  {%- if column_exists(direct_reference_path) -%}
    IF({{has_value(direct_reference_path)}}, 1, 0)
  {%- elif column_exists(indirect_reference_path) -%}
    IF({{reference_column}}.type = '{{reference_resource}}' AND {{has_value(indirect_reference_path)}}, 1, 0)
  {%- else -%}
    0
  {%- endif -%}

{%- endif -%}

{%- endmacro -%}