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
    materialized='view'
) -}}
{%- set models_dict = get_dbt_objects('seed') -%}
{%- set threshold_relations = [] -%}
{%- for name, path in models_dict.items()
  if name[:10] == 'thresholds'
-%}
  {%- set relation = adapter.get_relation(
    database = target.project,
    schema = target.schema,
    identifier = name
  ) %}
  {%- do threshold_relations.append(relation) %}
{%- endfor -%}
{{ dbt_utils.union_relations(
    relations = threshold_relations,
    source_column_name = "thresholds_source",
    column_override = {
      "metric_name": "STRING",
      "threshold_low": type_double(),
      "threshold_high": type_double(),
      "time_grain": "STRING",
      "dimension": "STRING",
      "validation_feature": "STRING",
      "severity": "STRING"
    }
) }}