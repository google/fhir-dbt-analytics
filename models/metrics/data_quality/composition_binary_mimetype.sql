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
      "description": "Count of Composition by Binary MIME Type",
      "short_description": "Composition Binary MIME Type",
      "primary_resource": "Composition",
      "primary_fields": ['Composition.section.entry'],
      "secondary_resources": ['Binary'],
      "calculation": "COUNT",
      "category": "Data distribution",
      "metric_date_field": "Composition.date",
      "metric_date_description": "Composition date",
      "dimension_a": "composition_status",
      "dimension_a_description": "The composition status  (preliminary | final | amended | entered-in-error)",
      "dimension_b": "mime_type",
      "dimension_b_description": "The media type of the document ({MIME types} | NULL)",
    }
) -}}


{%- set metric_sql -%}
    SELECT
      C.id,
      {{- metric_common_dimensions(table_alias='C') }}
      status as composition_status,
      B.contentType AS mime_type
    FROM {{ ref('Composition') }} AS C
    LEFT JOIN UNNEST(C.section) AS CS
    LEFT JOIN {{ ref('Binary') }} AS B
      ON B.id = CS.entry[SAFE_OFFSET(0)].binaryId
{%- endset -%}

{{ calculate_metric(
    metric_sql
) }}