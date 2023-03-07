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

-- depends_on: {{ ref('metric_all_definitions') }}

{{ config(
    alias = 'metric',
    materialized = 'table',
    post_hook = "{{ maybe_drop_metric_tables() }}"
) -}}

WITH

  Breakdowns AS (
    SELECT DISTINCT
      M.execution_id, M.execution_date, M.execution_datetime,
      M.metric_name, M.source_system, M.site, M.data_transfer_type, M.dimension_a, M.dimension_b, M.dimension_c, M.fhir_mapping,
      D.calculation, D.metric_date_field
      FROM {{ ref('metric_latest_execution') }} AS M
      JOIN {{ ref('metric_all_definitions') }} AS D ON M.metric_name = D.metric_name
  ),

  DateRange AS (
    SELECT

      {%- if var('static_dataset') %}
      GENERATE_DATE_ARRAY(
        PARSE_DATE("%F", "{{ var('earliest_date') }}"),
        PARSE_DATE("%F", "{{ var('latest_date') }}")
      ) AS date_array

      {%- else %}
      GENERATE_DATE_ARRAY(
        DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('months_history') }} MONTH),
        CURRENT_DATE()
      ) AS date_array
      {%- endif %}
  )

SELECT
  B.execution_id,
  B.execution_date,
  B.execution_datetime,
  B.metric_name,
  B.fhir_mapping,
  B.source_system,
  B.data_transfer_type,
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
LEFT JOIN DateRange AS DR ON B.metric_date_field != ''
LEFT JOIN UNNEST(DR.date_array) AS metric_date
LEFT JOIN {{ ref('metric_latest_execution') }} AS M
  ON B.metric_name = M.metric_name
  AND (metric_date = M.metric_date OR (metric_date IS NULL AND M.metric_date IS NULL))
  AND (B.source_system = M.source_system OR (B.source_system IS NULL AND M.source_system IS NULL))
  AND (B.data_transfer_type = M.data_transfer_type OR (B.data_transfer_type IS NULL AND M.data_transfer_type IS NULL))
  AND (B.site = M.site OR (B.site IS NULL AND M.site IS NULL))
  AND (B.dimension_a = M.dimension_a OR (B.dimension_a IS NULL AND M.dimension_a IS NULL))
  AND (B.dimension_b = M.dimension_b OR (B.dimension_b IS NULL AND M.dimension_b IS NULL))
  AND (B.dimension_c = M.dimension_c OR (B.dimension_c IS NULL AND M.dimension_c IS NULL))
  AND (B.fhir_mapping = M.fhir_mapping OR (B.fhir_mapping IS NULL AND M.fhir_mapping IS NULL))

{%- set this_relation = adapter.get_relation(this.database, this.schema, this.table) %}
{% if this_relation != None %}
UNION ALL
SELECT * FROM {{ this }} WHERE metric_name NOT IN (SELECT DISTINCT metric_name FROM {{ ref('metric_latest_execution') }})
{% endif %}