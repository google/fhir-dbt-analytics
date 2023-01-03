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
      "description": "Proportion of MedicationStatement resources that reference a non-existent encounter",
      "short_description": "MedStat ref. Enc - non-exist",
      "primary_resource": "MedicationStatement",
      "primary_fields": ['context.encounterId'],
      "secondary_resources": ['Encounter'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "MedicationStatement.dateAsserted",
      "metric_date_description": "Medication statement asserted date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the medication statement (active | completed | entered-in-error | intended | stopped | on-hold | unknown | not-taken)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      CASE WHEN
        context.encounterId IS NOT NULL
        AND context.encounterId <> ''
        AND NOT EXISTS(
          SELECT E.id
          FROM {{ ref('Encounter') }} AS E
          WHERE context.encounterId = E.id
        )
        THEN 1 ELSE 0 END AS reference_encounter_unresolved
    FROM {{ ref('MedicationStatement') }} AS M
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(reference_encounter_unresolved)',
    denominator = 'COUNT(id)'
) }}
