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
  D.primary_resource,
  ANY_VALUE(D.primary_fields) AS primary_fields,
  ANY_VALUE(D.secondary_resources) AS secondary_resources,
  D.calculation,
  D.category,
  M.source_system,
  M.site,
  SUM(M.numerator) AS numerator,
  SUM(M.denominator) AS denominator,
  {{ calculate_measure() }} AS measure
FROM {{ ref('metric_definition') }} AS D
JOIN {{ ref('metric') }} AS M USING(metric_name)
GROUP BY 1,2,3,6,7,8,9