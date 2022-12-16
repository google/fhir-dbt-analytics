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
      "description": "Count of valid Person resources",
      "short_description": "Person resources",
      "primary_resource": "Person",
      "primary_fields": ['id'],
      "calculation": "COUNT",
      "category": "Resource count",
      "secondary_resources": ['Encounter'],
      "metric_date_field": "Encounter.period.start",
      "metric_date_description": "Encounter start date of person's earliest clinical encounter",
      "dimension_a": "active",
      "dimension_a_description": "Whether this person's record is in active use",
    }
) -}}

-- depends_on: {{ ref('Person') }}
-- depends_on: {{ ref('Encounter') }}
{%- if fhir_resource_exists('Person') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions(exclude_col='metric_date') }}
      CAST(active AS STRING) AS active,
      MIN(
        (
          SELECT MIN(metric_date)
          FROM {{ ref('Encounter') }} AS E
          WHERE l.target.patientid = E.subject.patientId
          AND E.class.code NOT IN (
            'OTHER',
            'PRENC',
            'LAB',
            'UNKNOWN',
            'HIST'
          )
        )
      ) AS metric_date
    FROM {{ ref('Person') }} AS P,
    UNNEST(P.link) AS l
    GROUP BY 1, 2, 3, 4, 5, 6
  )
{{ calculate_metric() }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}