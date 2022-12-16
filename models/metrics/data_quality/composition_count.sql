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
      "description": "Count of valid Composition resources",
      "short_description": "Composition resources",
      "primary_resource": "Composition",
      "primary_fields": ['id'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Resource count",
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
      status
    FROM {{ ref('Composition') }}
  )
{{ calculate_metric() }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}