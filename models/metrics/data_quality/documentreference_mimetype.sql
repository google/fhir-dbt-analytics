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
      "description": "Count of DocumentReference by MIME Type",
      "short_description": "DocRef MIME Type",
      "primary_resource": "DocumentReference",
      "primary_fields": ['content'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Data distribution",
      "metric_date_field": "DocumentReference.date",
      "metric_date_description": "Document reference date",
      "dimension_a": "document_status",
      "dimension_a_description": "The document reference status  (current | superseded | entered-in-error)",
      "dimension_b": "mime_type",
      "dimension_b_description": "The media type of the binary (text/html | plain/text | application/rtf)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      D.id,
      {{- metric_common_dimensions() }}
      status as document_status,
      {%- if fhir_dbt_utils.field_exists('content.attachment.contentType') %}
      {{ fhir_dbt_utils.safe_offset("content", 0) }}.attachment.contentType AS mime_type
      {%- else %}
      NULL AS mime_type
      {%- endif %}
    FROM {{ ref('DocumentReference') }} AS D
{%- endset -%}

{{ calculate_metric(
    metric_sql
) }}