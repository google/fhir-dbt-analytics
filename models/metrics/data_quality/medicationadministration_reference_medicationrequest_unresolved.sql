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
      "description": "Proportion of MedicationAdministration resources that reference a non-existent medication request",
      "short_description": "MedAdmin ref. MedReq - non-exist",
      "primary_resource": "MedicationAdministration",
      "primary_fields": [
          'context.encounterId', 
          'request.medicationRequestId'],
      "secondary_resources": ['MedicationRequest'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "COALESCE(MedicationAdministration.effective.period.start, MedicationAdministration.effective.dateTime)",
      "metric_date_description": "MedicationAdministration effective period start date (if absent, MedicationAdministration effective date)",
      "dimension_a": "status",
      "dimension_a_description": "The status of the medication administration (in-progress | not-done | on-hold | completed | entered-in-error | stopped | unknown)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      CASE WHEN
        request.medicationRequestId IS NOT NULL
        AND request.medicationRequestId <> ''
        AND NOT EXISTS(
          SELECT MR.id
          FROM {{ ref('MedicationRequest') }} AS MR
          WHERE M.request.medicationRequestId = MR.id
        )
        THEN 1 ELSE 0 END AS reference_medicationrequest_unresolved    
    FROM {{ ref('MedicationAdministration') }} AS M
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(reference_medicationrequest_unresolved)',
    denominator = 'COUNT(id)'
) }}
