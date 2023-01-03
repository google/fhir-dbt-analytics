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
      "description": "Proportion of Composition resources that do not have a binary reference recorded",
      "short_description": "Comp ref. Binary - unrecorded",
      "primary_resource": "Composition",
      "primary_fields": ['section.entry.binaryId'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "Composition.date",
      "metric_date_description": "Composition latest edit date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the composition (preliminary | final | amended | entered-in-error)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      (
        SELECT SIGN(COUNT(*))
        FROM UNNEST(C.section) AS CS
        JOIN UNNEST(CS.entry) AS CSE
        WHERE CSE.binaryid IS NOT NULL
        AND CSE.binaryid <> ''
      ) AS has_reference_binary
    FROM {{ ref('Composition') }} AS C
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(1 - has_reference_binary)',
    denominator = 'COUNT(id)'
) }}
