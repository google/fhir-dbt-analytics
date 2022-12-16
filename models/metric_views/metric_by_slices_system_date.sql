{#
/* Copyright 2022 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#}

{{ config(
    materialized='view'
) -}}

SELECT
  D.metric_name,
  D.description,
  D.short_description,
  D.primary_resource,
  D.primary_fields,
  D.secondary_resources,
  D.category,
  D.calculation,
  D.metric_date_field,
  D.metric_date_description,
  {{ snake_case_to_proper_case('D.dimension_a') }} AS dimension_a_name,
  D.dimension_a_description,
  {{ snake_case_to_proper_case('D.dimension_b') }} AS dimension_b_name,
  D.dimension_b_description,
  {{ snake_case_to_proper_case('D.dimension_c') }} AS dimension_c_name,
  D.dimension_c_description,
  M.source_system,
  M.site,
  M.dimension_a,
  M.dimension_b,
  M.dimension_c,
  M.fhir_mapping,
  M.metric_date,
  EXTRACT(YEAR FROM M.metric_date) AS metric_year,
  M.numerator,
  M.denominator,
  M.measure
FROM {{ ref('metric_definition') }} AS D
JOIN {{ ref('metric') }} AS M USING(metric_name)
