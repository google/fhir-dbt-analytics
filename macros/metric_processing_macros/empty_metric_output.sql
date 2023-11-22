-- Copyright 2023 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

{%- macro empty_metric_output() %}
SELECT
  {{ fhir_dbt_utils.current_datetime() }} as execution_datetime,
  '{{this.name}}' AS metric_name,
  CAST(NULL AS DATE) AS metric_date,
  '{{var('site_default')}}' AS site,
  '{{var('source_system_default')}}' AS source_system,
  CAST(NULL AS STRING) AS fhir_mapping,
  CAST(NULL AS STRING) AS dimension_a,
  CAST(NULL AS STRING) AS dimension_b,
  CAST(NULL AS STRING) AS dimension_c,
  CAST(NULL AS {{ fhir_dbt_utils.type_long() }}) AS numerator,
  CAST(NULL AS {{ fhir_dbt_utils.type_long() }}) AS denominator,
  CAST(NULL AS {{ fhir_dbt_utils.type_double() }}) AS measure
{%- endmacro -%}