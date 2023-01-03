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
      "description": "Proportion of LDA Procedure resources that reference a non-existent practitioner",
      "short_description": "LDA Proc ref. Prac - non-exist",
      "primary_resource": "Procedure",
      "primary_fields": ['performer.actor.practitionerId'],
      "secondary_resources": ['Practitioner'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "COALESCE(Procedure.performed.period.start, Procedure.performed.dateTime)",
      "metric_date_description": "Procedure performed period start date (if absent, procedure performed date)",
      "dimension_a": "status",
      "dimension_a_description": "The status of the procedure (preparation | in-progress | not-done | on-hold | stopped | completed | entered-in-error | unknown)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      (
        SELECT SIGN(COUNT(*))
        FROM UNNEST(P.performer) AS PP
        WHERE actor.practitionerId IS NOT NULL
        AND actor.practitionerId <> ''
      ) AS has_reference_practitioner,
      (
        SELECT SIGN(COUNT(*))
        FROM UNNEST(P.performer) AS PP
        JOIN {{ ref('Practitioner') }} AS P
          ON PP.actor.practitionerId = P.id
      ) AS reference_practitioner_resolved
    FROM {{ ref('Procedure') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(has_reference_practitioner - reference_practitioner_resolved)',
    denominator = 'COUNT(id)'
) }}
