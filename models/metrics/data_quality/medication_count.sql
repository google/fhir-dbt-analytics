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
      "description": "Count of valid Medication resources",
      "short_description": "Medication resources",
      "primary_resource": "Medication",
      "primary_fields": ['id'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Resource count",
      "dimension_a": "status",
      "dimension_a_description": "The status of the medication (active | inactive | entered-in-error)",
    }
) -}}

-- depends_on: {{ ref('Medication') }}
{%- if fhir_resource_exists('Medication') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status
    FROM {{ ref('Medication') }}
  )
{{ calculate_metric() }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}