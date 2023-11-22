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

{% macro has_reference_value(
  reference_column,
  reference_resource
) %}

{%- set reference_paths = get_reference_paths(reference_column, reference_resource) -%}
{%- set direct_reference = reference_paths['direct_reference'] -%}
{%- set direct_reference_path = reference_paths['direct_reference_path'] -%}
{%- set indirect_reference_path = reference_paths['indirect_reference_path'] -%}
{%- set reference_column_is_array = reference_paths['reference_column_is_array']-%}

{%- if reference_column_is_array -%}

  {%- if fhir_dbt_utils.field_exists(direct_reference_path) -%}
    (SELECT SIGN(COUNT(*)) FROM {{ fhir_dbt_utils.spark_parenthesis(fhir_dbt_utils.unnest(reference_column, "RC")) }} WHERE {{fhir_dbt_utils.has_value("RC."~direct_reference)}})
  {%- elif fhir_dbt_utils.field_exists(indirect_reference_path) -%}
    (SELECT SIGN(COUNT(*)) FROM {{ fhir_dbt_utils.spark_parenthesis(fhir_dbt_utils.unnest(reference_column, "RC")) }} WHERE RC.type = '{{reference_resource}}' AND {{fhir_dbt_utils.has_value('RC.reference')}})
  {%- else -%}
    0
  {%- endif -%}

{%- else -%}

  {%- if fhir_dbt_utils.field_exists(direct_reference_path) -%}
    IF({{fhir_dbt_utils.has_value(direct_reference_path)}}, 1, 0)
  {%- elif fhir_dbt_utils.field_exists(indirect_reference_path) -%}
    IF({{reference_column}}.type = '{{reference_resource}}' AND {{fhir_dbt_utils.has_value(indirect_reference_path)}}, 1, 0)
  {%- else -%}
    0
  {%- endif -%}

{%- endif -%}

{%- endmacro -%}