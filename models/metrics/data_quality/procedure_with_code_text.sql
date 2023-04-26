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
      "description": "Proportion of Procedure resources that contain a non-empty code.text",
      "short_description": "Procedure with code.text",
      "primary_resource": "Procedure",
      "primary_fields": ['code.text'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Data completeness",
      "metric_date_field": "COALESCE(performed.period.start, performed.dateTime)",
      "metric_date_description": "Procedure performed datetime",
      "dimension_a": "status",
      "dimension_a_description": "The procedure status bound to http://hl7.org/fhir/event-status",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      {{ has_value('P.code.text') }} AS has_code_text
    FROM {{ ref('Procedure') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(CAST(has_code_text AS INT64))',
    denominator = 'COUNT(id)'
) }}