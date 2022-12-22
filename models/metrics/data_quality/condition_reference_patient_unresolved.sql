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
      "description": "Proportion of Condition resources that do not reference an existing patient",
      "short_description": "Cond ref. Patient - non-exist",
      "primary_resource": "Condition",
      "primary_fields": ['subject.patientId'],
      "secondary_resources": ['Patient'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "Condition.recordedDate",
      "metric_date_description": "Condition recorded date",
      "dimension_a": "clinical_status",
      "dimension_a_description": "The clinical status of the condition (active | recurrence | relapse | inactive | remission | resolved)",
      "dimension_b": "verification_status",
      "dimension_b_description": "The verification status of the condition (unconfirmed | provisional | differential | confirmed | refuted | entered-in-error)",
      "dimension_c": "category",
      "dimension_c_description": "The category of the condition (problem-list-item | encounter-diagnosis)",
    }
) -}}

-- depends_on: {{ ref('Condition') }}
-- depends_on: {{ ref('Patient') }}
{%- if fhir_resource_exists('Condition') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      {{ try_code_from_codeableconcept('clinicalStatus', 'http://terminology.hl7.org/CodeSystem/condition-clinical') }} AS clinical_status,
      {{ try_code_from_codeableconcept('verificationStatus', 'http://terminology.hl7.org/CodeSystem/condition-ver-status') }} AS verification_status,
      {{ try_code_from_codeableconcept('category', 'http://terminology.hl7.org/CodeSystem/condition-category', index = 0) }} AS category,
      CASE WHEN
        NOT EXISTS(
          SELECT P.id
          FROM {{ ref('Patient') }} AS P
          WHERE C.subject.patientId = P.id
        )
        THEN 1 ELSE 0 END AS reference_patient_unresolved
    FROM {{ ref('Condition') }} AS C
  )
{{ calculate_metric(
    numerator = 'SUM(reference_patient_unresolved)',
    denominator = 'COUNT(id)'
) }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}