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
      "description": "Count of valid Patient resources",
      "short_description": "Patient resources",
      "primary_resource": "Patient",
      "primary_fields": ['id'],
      "secondary_resources": ['Encounter'],
      "calculation": "COUNT",
      "category": "Resource count",
      "metric_date_field": "Encounter.period.start",
      "metric_date_description": "Encounter start date of patient's earliest clinical encounter",
      "dimension_a_name": "Active",
      "dimension_a_description": "Whether this patient's record is in active use",
      "dimension_b_name": "Gender",
      "dimension_b_description": "The patient's gender (male, female, other, unknown)",
    }
) -}}

WITH
  A AS (
    SELECT
      id, 
      (
        SELECT MIN(metric_date)
        FROM {{ ref('Encounter') }} AS E
        WHERE P.id = E.subject.patientId
        AND E.class.code NOT IN (
          'OTHER',
          'PRENC',
          'LAB',
          'UNKNOWN',
          'HIST'
        )
      ) AS metric_date,
      fhir_mapping,
      source_system,
      site,
      data_transfer_type,
      CAST({{ get_column_or_default('active') }} AS STRING) AS active,
      {{ get_column_or_default('gender') }} AS gender,
    FROM {{ ref('Patient') }} AS P
  )
SELECT
  CURRENT_DATETIME() as execution_datetime,
  '{{this.name}}' AS metric_name,
  fhir_mapping AS fhir_mapping,
  source_system AS source_system,
  data_transfer_type AS data_transfer_type,
  metric_date AS metric_date,
  site AS site,
  CAST(active AS STRING) AS slice_a,
  CAST(gender AS STRING) AS slice_b,
  CAST(NULL AS STRING) AS slice_c,
  NULL AS numerator,
  COUNT(DISTINCT id) AS denominator_cohort,
  CAST(COUNT(DISTINCT id) AS FLOAT64) AS measure
FROM A
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10