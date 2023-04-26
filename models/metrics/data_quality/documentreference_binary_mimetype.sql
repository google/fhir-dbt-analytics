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
      "description": "Count of DocumentReference by Binary MIME Type",
      "short_description": "DocRef Binary MIME Type",
      "primary_resource": "DocumentReference",
      "primary_fields": ['content'],
      "secondary_resources": ['Binary'],
      "calculation": "COUNT",
      "category": "Data distribution",
      "metric_date_field": "DocumentReference.date",
      "metric_date_description": "Document reference date",
      "dimension_a": "document_status",
      "dimension_a_description": "The document reference status  (current | superseded | entered-in-error)",
      "dimension_b": "mime_type",
      "dimension_b_description": "The media type of the document (binary | image/png | NULL)",
    }
) -}}


{%- set metric_sql -%}
    SELECT
      D.id,
      {{- metric_common_dimensions(table_alias='D') }}
      status as document_status,
      B.contentType AS mime_type
    FROM {{ ref('DocumentReference') }} AS D
    LEFT JOIN UNNEST(D.content) AS C
    LEFT JOIN {{ ref('Binary') }} AS B
      ON B.id = REPLACE(C.attachment.url, 'Binary/', '')
{%- endset -%}

{{ calculate_metric(
    metric_sql
) }}