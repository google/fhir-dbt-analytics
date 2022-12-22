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
      "description": "Proportion of LDA Procedure resources that do not reference an existing patient",
      "short_description": "LDA Proc ref. Patient - non-exist",
      "primary_resource": "Procedure",
      "primary_fields": ['subject.patientId'],
      "secondary_resources": ['Patient'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "COALESCE(Procedure.performed.period.start, Procedure.performed.dateTime)",
      "metric_date_description": "Procedure performed period start date (if absent, procedure performed date)",
      "dimension_a": "status",
      "dimension_a_description": "The status of the procedure (preparation | in-progress | not-done | on-hold | stopped | completed | entered-in-error | unknown)",
    }
) -}}

-- depends_on: {{ ref('Procedure') }}
-- depends_on: {{ ref('Patient') }}
{%- if fhir_resource_exists('Procedure') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      CASE WHEN
        NOT EXISTS(
          SELECT Pat.id
          FROM {{ ref('Patient') }} AS Pat
          WHERE P.subject.patientId = Pat.id
        )
        THEN 1 ELSE 0 END AS reference_patient_unresolved
    FROM {{ ref('Procedure') }} AS P
  )
{{ calculate_metric(
    numerator = 'SUM(reference_patient_unresolved)',
    denominator = 'COUNT(id)'
) }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}