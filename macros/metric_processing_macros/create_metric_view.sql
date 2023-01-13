{% macro create_metric_view(include_date, include_slices) -%}

SELECT
 {#- base (system) segment, present in all views #}
  D.metric_name,
  D.description,
  D.short_description,
  D.primary_resource,
  ARRAY_TO_STRING(D.primary_fields, ",") AS primary_fields,
  ARRAY_TO_STRING(D.secondary_resources, ",") AS secondary_resources,
  D.category,
  D.calculation,
  M.source_system,
  M.site,
  M.fhir_mapping,

  {%- if include_date -%}
  {# date segment, present in date and slices views #}
  D.metric_date_field,
  D.metric_date_description,
  M.metric_date,
  EXTRACT(YEAR FROM M.metric_date) AS metric_year,
  {%- endif -%}

  {%- if include_slices -%}
  {# slices segment, present in slices view #}
  {{ snake_case_to_proper_case('D.dimension_a') }} AS dimension_a_name,
  D.dimension_a_description,
  {{ snake_case_to_proper_case('D.dimension_b') }} AS dimension_b_name,
  D.dimension_b_description,
  {{ snake_case_to_proper_case('D.dimension_c') }} AS dimension_c_name,
  D.dimension_c_description,
  M.dimension_a,
  M.dimension_b,
  M.dimension_c,
  {%- endif -%}

  {# measure calc segment #}
  SUM(M.numerator) AS numerator,
  SUM(M.denominator) AS denominator,
  {{ calculate_measure() }} AS measure
{# source tables, join & group -#}
FROM {{ ref('metric_definition') }} AS D
JOIN {{ ref('metric') }} AS M USING(metric_name)
GROUP BY
metric_name, description, short_description, primary_resource, primary_fields, secondary_resources,
category, calculation, source_system, site, fhir_mapping

  {%- if include_date -%}
  , metric_date_field, metric_date_description, metric_date, metric_year
  {%- endif -%}

  {%- if include_slices -%}
  , dimension_a_name, dimension_a_description, dimension_b_name, dimension_b_description,
  dimension_c_name, dimension_c_description, dimension_a, dimension_b, dimension_c
  {%- endif -%}

{%- endmacro %}