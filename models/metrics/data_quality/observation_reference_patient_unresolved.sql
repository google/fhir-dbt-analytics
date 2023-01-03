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
      "description": "Proportion of Observation resources that do not reference an existing patient",
      "short_description": "Obs ref. Patient - non-exist",
      "primary_resource": "Observation",
      "primary_fields": ['subject.patientId'],
      "secondary_resources": ['Patient'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "Observation.effective.dateTime",
      "metric_date_description": "Observation effective date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the observation (registered | preliminary | final | amended +)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      CASE WHEN
        NOT EXISTS(
          SELECT P.id
          FROM {{ ref('Patient') }} AS P
          WHERE O.subject.patientId = P.id
        )
        THEN 1 ELSE 0 END AS reference_patient_unresolved
    FROM {{ ref('Observation') }} AS O
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(reference_patient_unresolved)',
    denominator = 'COUNT(id)'
) }}
