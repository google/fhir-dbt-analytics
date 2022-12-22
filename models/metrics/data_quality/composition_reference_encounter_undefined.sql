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
      "description": "Proportion of Composition resources that do not have an encounter reference recorded",
      "short_description": "Comp ref. Enc - unrecorded",
      "primary_resource": "Composition",
      "primary_fields": ['encounter.encounterId'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "Composition.date",
      "metric_date_description": "Composition latest edit date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the composition (preliminary | final | amended | entered-in-error)",
    }
) -}}

-- depends_on: {{ ref('Composition') }}
{%- if fhir_resource_exists('Composition') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      CASE WHEN encounter.encounterId IS NULL OR encounter.encounterId = '' THEN 1 ELSE 0 END AS reference_encounter_undefined
    FROM {{ ref('Composition') }} AS C
  )
{{ calculate_metric(
    numerator = 'SUM(reference_encounter_undefined)',
    denominator = 'COUNT(id)'
) }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}