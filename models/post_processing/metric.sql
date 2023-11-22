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
    materialized = 'table',
    post_hook = "{{ maybe_drop_metric_tables() }}"
) -}}

WITH

  Breakdowns AS (
    SELECT DISTINCT
      M.execution_id, M.execution_date, M.execution_datetime,
      M.metric_name, M.source_system, M.site, M.dimension_a, M.dimension_b, M.dimension_c, M.fhir_mapping,
      D.calculation, D.metric_date_field
      FROM {{ ref('metric_latest_execution') }} AS M
      JOIN {{ ref('metric_all_definitions') }} AS D ON M.metric_name = D.metric_name
  ),
  DS AS (
    {{ date_spine() }}
  )

SELECT
  B.execution_id,
  B.execution_date,
  B.execution_datetime,
  B.metric_name,
  B.fhir_mapping,
  B.source_system,
  CAST(metric_date AS DATE) AS metric_date,
  B.site,
  B.dimension_a,
  B.dimension_b,
  B.dimension_c,

  -- Impute different values for rows without data for count metrics and non-count metrics:
  COALESCE(M.numerator, IF(B.calculation IN ('COUNT'), NULL, 0)) AS numerator,
  COALESCE(M.denominator, IF(B.calculation IN ('COUNT'), NULL, 0)) AS denominator,
  COALESCE(M.measure, IF(B.calculation IN ('COUNT'), 0, NULL)) AS measure

FROM Breakdowns AS B
LEFT JOIN DS ON B.metric_date_field != ''
LEFT JOIN {{ ref('metric_latest_execution') }} AS M
  ON B.metric_name = M.metric_name
  AND (DS.date_day = M.metric_date OR (DS.date_day IS NULL AND M.metric_date IS NULL))
  AND (B.source_system = M.source_system OR (B.source_system IS NULL AND M.source_system IS NULL))
  AND (B.site = M.site OR (B.site IS NULL AND M.site IS NULL))
  AND (B.dimension_a = M.dimension_a OR (B.dimension_a IS NULL AND M.dimension_a IS NULL))
  AND (B.dimension_b = M.dimension_b OR (B.dimension_b IS NULL AND M.dimension_b IS NULL))
  AND (B.dimension_c = M.dimension_c OR (B.dimension_c IS NULL AND M.dimension_c IS NULL))
  AND (B.fhir_mapping = M.fhir_mapping OR (B.fhir_mapping IS NULL AND M.fhir_mapping IS NULL))

{# Use a variable to allow dbt to discover the `ref`s. And use `metric_copy` because Spark first
   drops the table before running the model. #}
{% set union_metric_copy %}
UNION ALL
SELECT * FROM {{ ref('metric_copy') }}
WHERE metric_name NOT IN (SELECT DISTINCT metric_name FROM {{ ref('metric_latest_execution') }})
{% endset %}

{% if adapter.get_relation(this.database, this.schema, this.table) %}
{{ union_metric_copy }}
{% endif %}