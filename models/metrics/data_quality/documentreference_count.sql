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
      "description": "Count of valid DocumentReference resources",
      "short_description": "DocumentReference resources",
      "primary_resource": "DocumentReference",
      "primary_fields": ['id'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Resource count",
      "metric_date_field": "DocumentReference.date",
      "metric_date_description": "Document reference date",
      "dimension_a": "document_status",
      "dimension_a_description": "The document reference status  (current | superseded | entered-in-error)",
      "dimension_b": "Format",
      "dimension_b_description": "The format of the document referenced (scanned | NULL)",
    }
) -}}

-- depends_on: {{ ref('DocumentReference') }}
{%- if fhir_resource_exists('DocumentReference') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status as document_status,
      {%- if column_exists('content.format.code') %}
      content[SAFE_OFFSET(0)].format.code AS format
      {%- else %}
      NULL AS format
      {%- endif %}
    FROM {{ ref('DocumentReference') }} AS DR
  )
{{ calculate_metric() }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}