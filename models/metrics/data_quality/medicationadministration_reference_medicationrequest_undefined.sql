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
      "description": "Proportion of MedicationAdministration resources that do not have an medication request reference recorded",
      "short_description": "MedAdmin ref. MedReq - unrecorded",
      "primary_resource": "MedicationAdministration",
      "primary_fields": ['context.encounterId'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "COALESCE(MedicationAdministration.effective.period.start, MedicationAdministration.effective.dateTime)",
      "metric_date_description": "MedicationAdministration effective period start date (if absent, MedicationAdministration effective date)",
      "dimension_a": "status",
      "dimension_a_description": "The status of the medication administration (in-progress | not-done | on-hold | completed | entered-in-error | stopped | unknown)",
    }
) -}}

-- depends_on: {{ ref('MedicationAdministration') }}
{%- if fhir_resource_exists('MedicationAdministration') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      CASE WHEN request.medicationRequestId IS NULL OR request.medicationRequestId = '' THEN 1 ELSE 0 END AS reference_medicationrequest_undefined
    FROM {{ ref('MedicationAdministration') }} AS M
  )
{{ calculate_metric(
    numerator = 'SUM(reference_medicationrequest_undefined)',
    denominator = 'COUNT(id)'
) }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}