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

{# Copy the `metric` data to allow the `metric` model to backfill what wasn't run in the latest
   execution. #}
{{ config(
    materialized = 'table',
) }}

{% if adapter.get_relation(this.database, this.schema, 'metric') %}
{# Can't use `ref` because that would cause a cycle. #}
SELECT
  execution_id,
  execution_date,
  execution_datetime,
  metric_name,
  fhir_mapping,
  source_system,
  metric_date,
  site,
  dimension_a,
  dimension_b,
  dimension_c,
  numerator,
  denominator,
  measure
FROM {{ fhir_dbt_utils.table_ref(this.database, this.schema, 'metric') }}
{% else %}
SELECT "not found" AS dummy
{% endif %}