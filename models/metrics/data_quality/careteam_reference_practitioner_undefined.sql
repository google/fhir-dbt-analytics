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
      "description": "Proportion of CareTeam resources that do not have a practitioner reference recorded",
      "short_description": "CareTeam ref. Prac - unrecorded",
      "primary_resource": "CareTeam",
      "primary_fields": ['participant.member.practitionerId'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "CareTeam.period.start",
      "metric_date_description": "Start of time period covered by care team",
      "dimension_a": "status",
      "dimension_a_description": "The status of the care team (proposed | active | suspended | inactive | entered-in-error)",
    }
) -}}

-- depends_on: {{ ref('CareTeam') }}
{%- if fhir_resource_exists('CareTeam') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      (
        SELECT SIGN(COUNT(*))
        FROM UNNEST(C.participant) AS CP
        WHERE member.practitionerId IS NOT NULL
        AND member.practitionerId <> ''
      ) AS has_reference_practitioner
    FROM {{ ref('CareTeam') }} AS C
  )
{{ calculate_metric(
    numerator = 'SUM(1 - has_reference_practitioner)',
    denominator = 'COUNT(id)'
) }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}