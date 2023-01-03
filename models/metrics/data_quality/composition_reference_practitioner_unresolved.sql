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
      "description": "Proportion of Composition resources that reference a non-existent practitioner",
      "short_description": "Comp ref. Prac - non-exist",
      "primary_resource": "Composition",
      "primary_fields": ['author.practitionerId'],
      "secondary_resources": ['Practitioner'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "Composition.date",
      "metric_date_description": "Composition latest edit date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the composition (preliminary | final | amended | entered-in-error)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      (
        SELECT SIGN(COUNT(*))
        FROM UNNEST(C.author) AS CA
        WHERE practitionerId IS NOT NULL
        AND practitionerId <> ''
      ) AS has_reference_practitioner,
      (
        SELECT SIGN(COUNT(*))
        FROM UNNEST(C.author) AS CA
        JOIN {{ ref('Practitioner') }} AS P
          ON CA.practitionerId = P.id
      ) AS reference_practitioner_resolved
    FROM {{ ref('Composition') }} AS C
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(has_reference_practitioner - reference_practitioner_resolved)',
    denominator = 'COUNT(id)'
) }}
