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
      "description": "Count of valid Binary resources",
      "short_description": "Binary resources",
      "primary_resource": "Binary",
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

{%- set metric_sql -%}
    SELECT
      B.id,
      {{- metric_common_dimensions(table_alias='B', include_metric_date=False) }}
      C.metric_date,
      {{ get_column_or_default('status', 'Composition', table_alias='C') }} AS status
    FROM {{ ref('Binary') }} AS B
    LEFT JOIN {{ ref('Composition') }} AS C
    {%- if fhir_dbt_utils.field_exists('section.entry.binaryId', 'Composition') %}
      ON B.id = (SELECT binaryId FROM UNNEST((SELECT entry FROM UNNEST(section))))
    {%- else %}
      ON FALSE
    {%- endif -%}
{%- endset -%}

{{ calculate_metric(metric_sql) }}