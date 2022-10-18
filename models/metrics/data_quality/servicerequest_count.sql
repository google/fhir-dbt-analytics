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
      "description": "Count of valid ServiceRequest resources",
      "short_description": "ServiceRequest resources",
      "primary_resource": "ServiceRequest",
      "primary_fields": ['id'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Resource count",
      "metric_date_field": "ServiceRequest.authoredOn",
      "metric_date_description": "Service request signed date",
      "dimension_a_name": "Status",
      "dimension_a_description": "The status of the service request (draft | active | on-hold | revoked | completed | entered-in-error | unknown)",
      "dimension_b_name": "Intent",
      "dimension_b_description": "The intent of the service request (proposal | plan | directive | order | original-order | reflex-order | filler-order | instance-order | option)",
      "dimension_c_name": "Category",
      "dimension_c_description": "The category of the service request",
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
      {{ get_column_or_default('intent') }} AS intent,
      {{ try_code_from_codeableconcept('category', 'http://snomed.info/sct', index = 0, return_display = True) }} AS category
    FROM {{ ref('ServiceRequest') }}
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
  CAST(intent AS STRING) AS slice_b,
  CAST(category AS STRING) AS slice_c,
  NULL AS numerator,
  COUNT(DISTINCT id) AS denominator_cohort,
  CAST(COUNT(DISTINCT id) AS FLOAT64) AS measure
FROM A
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10