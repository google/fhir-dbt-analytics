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
      "description": "Proportion of MedicationStatement resources that do not have an medication reference recorded",
      "short_description": "MedStat ref. Med - unrecorded",
      "primary_resource": "MedicationStatement",
      "primary_fields": ['medication.reference.medicationId'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "MedicationStatement.dateAsserted",
      "metric_date_description": "Medication statement asserted date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the medication statement (active | completed | entered-in-error | intended | stopped | on-hold | unknown | not-taken)",
    }
) -}}

-- depends_on: {{ ref('MedicationStatement') }}
{%- if fhir_resource_exists('MedicationStatement') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      CASE WHEN medication.reference.medicationId IS NULL OR medication.reference.medicationId = '' THEN 1 ELSE 0 END AS reference_medication_undefined
    FROM {{ ref('MedicationStatement') }} AS M
  )
{{ calculate_metric(
    numerator = 'SUM(reference_medication_undefined)',
    denominator = 'COUNT(id)'
) }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}