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

-- depends_on: {{ ref('metric_all_definitions') }}

{{ config(
    materialized='table'
) -}}

WITH

UnionedMetrics AS (
  {{ union_metric_tables() }}
),

Execution AS (
  SELECT {{ fhir_dbt_utils.uuid() }} AS execution_id,
  CURRENT_DATE() AS execution_date
)

SELECT
  Execution.execution_id,
  Execution.execution_date,
  UnionedMetrics.*
FROM UnionedMetrics
CROSS JOIN Execution
