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
      "description": "Proportion of MedicationRequest resources that reference a non-existent medication",
      "short_description": "MedReq ref. Med - non-exist",
      "primary_resource": "MedicationRequest",
      "primary_fields": ['requester.practitionerId'],
      "secondary_resources": ['Medication'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "MedicationRequest.authoredOn",
      "metric_date_description": "Medication request initial authored date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the medication request (active | on-hold | cancelled | completed | entered-in-error | stopped | draft | unknown)",
      "dimension_b": "category",
      "dimension_b_description": "The category of the medication request (inpatient | outpatient | community | discharge)",
      "dimension_c": "intent",
      "dimension_c_description": "The intent of the medication request (proposal | plan | order | original-order | reflex-order | filler-order | instance-order | option)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      intent,
      {{ try_code_from_codeableconcept(
        'category',
        'http://terminology.hl7.org/CodeSystem/medicationrequest-category',
        index = get_source_specific_category_index()
      ) }} AS category,
      CASE WHEN
        medication.reference.medicationId IS NOT NULL
        AND medication.reference.medicationId <> ''
        AND NOT EXISTS(
          SELECT Med.id
          FROM {{ ref('Medication') }} AS Med
          WHERE M.medication.reference.medicationId = Med.id
        )
        THEN 1 ELSE 0 END AS reference_medication_unresolved
    FROM {{ ref('MedicationRequest') }} AS M
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(reference_medication_unresolved)',
    denominator = 'COUNT(id)'
) }}
