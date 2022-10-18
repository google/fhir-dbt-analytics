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
      "description": "Count of valid Person resources",
      "short_description": "Person resources",
      "primary_resource": "Person",
      "primary_fields": ['id'],
      "calculation": "COUNT",
      "category": "Resource count",
      "secondary_resources": ['Encounter'],
      "metric_date_field": "Encounter.period.start",
      "metric_date_description": "Encounter start date of person's earliest clinical encounter",
      "dimension_a_name": "Active",
      "dimension_a_description": "Whether this person's record is in active use",
    }
) -}}

WITH
  A AS (
    SELECT
      id,
      source_system,
      site,
      fhir_mapping,
      data_transfer_type,
      CAST({{ get_column_or_default('active') }} AS STRING) AS active,
      MIN(
        (
          SELECT MIN(metric_date)
          FROM {{ ref('Encounter') }} AS E
          WHERE l.target.patientid = E.subject.patientId
          AND E.class.code NOT IN (
            'OTHER',
            'PRENC',
            'LAB',
            'UNKNOWN',
            'HIST'
          )
        )
      ) AS metric_date
    FROM {{ ref('Person') }} AS P,
    UNNEST(P.link) AS l
    GROUP BY 1, 2, 3, 4, 5, 6
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
  CAST(NULL AS STRING) AS slice_b,
  CAST(NULL AS STRING) AS slice_c,
  NULL AS numerator,
  COUNT(DISTINCT id) AS denominator_cohort,
  CAST(COUNT(DISTINCT id) AS FLOAT64) AS measure
FROM A
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10