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
      "description": "Proportion of observations with a value that have a unit recorded",
      "short_description": "Obs value unit recorded",
      "primary_resource": "Observation",
      "primary_fields": ['value.quantity.unit'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Data completeness",
      "metric_date_field": "Observation.effective.dateTime",
      "metric_date_description": "Observation effective date",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      {{- metric_common_dimensions() }}
      O.id,
      IF(O.value.quantity.unit IS NOT NULL AND O.value.quantity.unit <> '', 1, 0) AS has_value_quantity_unit
    FROM {{ ref('Observation') }} AS O
    WHERE O.value.quantity.value IS NOT NULL
{%- endset -%}

{{- calculate_metric(
    metric_sql,
    numerator = 'SUM(has_value_quantity_unit)',
    denominator = 'COUNT(id)'
) -}}
