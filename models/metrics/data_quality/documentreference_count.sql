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
      "description": "Count of valid DocumentReference resources",
      "short_description": "DocumentReference resources",
      "primary_resource": "DocumentReference",
      "primary_fields": ['id'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Resource count",
      "metric_date_field": "DocumentReference.date",
      "metric_date_description": "Document reference date",
      "dimension_a_name": "Document status",
      "dimension_a_description": "The document reference status  (current | superseded | entered-in-error)",
      "dimension_b_name": "Format",
      "dimension_b_description": "The format of the document referenced (scanned | NULL)",
    }
) -}}

WITH
  A AS (
    SELECT
      id,
      fhir_mapping,
      metric_date,
      source_system,
      site,
      data_transfer_type,
      {{ get_column_or_default('status') }} AS status,
      {%- if column_exists('content.format.code') %}
      content[SAFE_OFFSET(0)].format.code AS format
      {%- else %}
      NULL AS format
      {%- endif %}
    FROM {{ ref('DocumentReference') }} AS DR
  )
SELECT
  CURRENT_DATETIME() as execution_datetime,
  '{{this.name}}' AS metric_name,
  fhir_mapping AS fhir_mapping,
  source_system AS source_system,
  data_transfer_type AS data_transfer_type,
  metric_date AS metric_date,
  site AS site,
  CAST(status AS STRING) AS slice_a,
  CAST(format AS STRING) AS slice_b,
  CAST(NULL AS STRING) AS slice_c,
  NULL AS numerator,
  COUNT(DISTINCT id) AS denominator_cohort,
  CAST(COUNT(DISTINCT id) AS FLOAT64) AS measure
FROM A
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10