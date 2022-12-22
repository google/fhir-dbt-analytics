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
      "description": "Proportion of DiagnosticReport resources that reference a non-existent observation",
      "short_description": "DiagRep ref. Obs - non-exist",
      "primary_resource": "DiagnosticReport",
      "primary_fields": ['result.observationId'],
      "secondary_resources": ['Observation'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "DiagnosticReport.issued",
      "metric_date_description": "Diagnostic report latest version issue date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the diagnostic report (registered | partial | preliminary | final +)",
      "dimension_b": "category",
      "dimension_b_description": "The service category of the diagnostic report",
    }
) -}}

-- depends_on: {{ ref('DiagnosticReport') }}
-- depends_on: {{ ref('Observation') }}
{%- if fhir_resource_exists('DiagnosticReport') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      {{ try_code_from_codeableconcept(
        'category',
        'https://g.co/fhir/harmonized/diagnostic_report/category',
        index = get_source_specific_category_index()
      ) }} AS category,
      (
        SELECT SIGN(COUNT(*))
        FROM UNNEST(D.result) AS DB
        WHERE observationId IS NOT NULL
        AND observationId <> ''
      ) AS has_reference_observation,
      (
        SELECT SIGN(COUNT(*))
        FROM UNNEST(D.result) AS DB
        JOIN {{ ref('Observation') }} AS O
          ON DB.observationId = O.id
      ) AS reference_observation_resolved
    FROM {{ ref('DiagnosticReport') }} AS D
  )
{{ calculate_metric(
    numerator = 'SUM(has_reference_observation - reference_observation_resolved)',
    denominator = 'COUNT(id)'
) }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}