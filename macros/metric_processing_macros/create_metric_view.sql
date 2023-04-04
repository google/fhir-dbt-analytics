{% macro create_metric_view(
  segment_by_date=None,
  segment_by_dimensions=None
) -%}

{%- if segment_by_dimensions == None -%}
  {%- set segment_by_dimensions = [] -%}
{%- endif -%}

{%- set group_by_count = 11 -%}

SELECT

{#- base segments, present in all views #}
  D.metric_name,
  D.description,
  D.short_description,
  D.primary_resource,
  {{ array_join("D.primary_fields", ",") }} AS primary_fields,
  {{ array_join("D.secondary_resources", ",") }} AS secondary_resources,
  D.category,
  D.calculation,
  M.source_system,
  M.site,
  M.fhir_mapping,


{#- date segments -#}
{%- if segment_by_date %}
  {%- set group_by_count = group_by_count + 2 %}
  D.metric_date_field,
  D.metric_date_description,
{%- endif -%}

{%- if segment_by_date == 'YEAR' %}
  {%- set group_by_count = group_by_count + 1 %}
  EXTRACT(YEAR FROM M.metric_date) AS metric_year,
{%- endif -%}

{%- if segment_by_date == 'DAY' %}
  {%- set group_by_count = group_by_count + 2 %}
  EXTRACT(YEAR FROM M.metric_date) AS metric_year,
  M.metric_date,
{%- endif -%}


{#- dimension segments -#}
{%- if 'dimension_a' in segment_by_dimensions %}
  {%- set group_by_count = group_by_count + 3 %}
  {{ snake_case_to_proper_case('D.dimension_a') }} AS dimension_a_name,
  D.dimension_a_description,
  M.dimension_a,
{%- endif -%}

{%- if 'dimension_b' in segment_by_dimensions %}
  {%- set group_by_count = group_by_count + 3 %}
  {{ snake_case_to_proper_case('D.dimension_b') }} AS dimension_b_name,
  D.dimension_b_description,
  M.dimension_b,
{%- endif -%}

{%- if 'dimension_c' in segment_by_dimensions %}
  {%- set group_by_count = group_by_count + 3 %}
  {{ snake_case_to_proper_case('D.dimension_c') }} AS dimension_c_name,
  D.dimension_c_description,
  M.dimension_c,
{%- endif -%}


{#- measure calculation #}
  SUM(M.numerator) AS numerator,
  SUM(M.denominator) AS denominator,
  {{ calculate_measure() }} AS measure

FROM {{ ref('metric_definition') }} AS D
JOIN {{ ref('metric') }} AS M USING(metric_name)

{{ dbt_utils.group_by(group_by_count)|upper }}
{%- endmacro %}