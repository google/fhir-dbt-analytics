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
    meta = {
      "description": "Proportion of observations with a value recorded",
      "short_description": "Obs value recorded",
      "primary_resource": "Observation",
      "primary_fields": ['value.quantity.value'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Data completeness",
      "metric_date_field": "Observation.effective.dateTime",
      "metric_date_description": "Observation effective date",
    }
) -}}

-- depends_on: {{ ref('Observation') }}
{%- if fhir_resource_exists('Observation') %}

WITH
  A AS (
    SELECT
      id,
      fhir_mapping,
      metric_date,
      source_system,
      site,
      data_transfer_type,
      IF({{ get_column_or_default('value.quantity.value') }} IS NOT NULL, 1, 0) AS has_value_quantity_value
    FROM {{ ref('Observation') }}
  )
SELECT
  CURRENT_DATETIME() as execution_datetime,
  '{{this.name}}' AS metric_name,
  fhir_mapping AS fhir_mapping,
  source_system AS source_system,
  data_transfer_type AS data_transfer_type,
  metric_date AS metric_date,
  site AS site,
  CAST(NULL AS STRING) AS slice_a,
  CAST(NULL AS STRING) AS slice_b,
  CAST(NULL AS STRING) AS slice_c,
  SUM(has_value_quantity_value) AS numerator,
  COUNT(id) AS denominator_cohort,
  CAST(SAFE_DIVIDE(SUM(has_value_quantity_value), COUNT(id)) AS FLOAT64) AS measure
FROM A
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}