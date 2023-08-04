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

{% macro metric_output(numerator=None, denominator=None, measure=None) %}

  {%- set dimension_a = model_metadata('dimension_a', value_if_missing='NULL') -%}
  {%- set dimension_b = model_metadata('dimension_b', value_if_missing='NULL') -%}
  {%- set dimension_c = model_metadata('dimension_c', value_if_missing='NULL') -%}

  {%- if model_metadata(meta_key='calculation') == 'COUNT' -%}
    {%- set numerator = 'CAST(NULL AS ' ~ type_long() ~ ')' -%}
    {%- set denominator = 'CAST(NULL AS ' ~ type_long() ~ ')' -%}
    {%- if measure == None -%}
      {%- set measure = 'CAST(COUNT(DISTINCT id) AS ' ~ type_double() ~ ')' -%}
    {%- endif -%}
  {%- endif -%}
  {%- if model_metadata(meta_key='calculation') in ['PROPORTION', 'RATIO'] -%}
    {%- if measure == None -%}
      {%- set measure = 'CAST(' ~ safe_divide(numerator, denominator) ~ ' AS ' ~ type_double() ~ ')' -%}
    {%- endif -%}
  {%- endif -%}
SELECT
  {{ current_datetime() }} as execution_datetime,
  '{{this.name}}' AS metric_name,
  {{- metric_common_dimensions() }}
  CAST({{ dimension_a }} AS STRING) AS dimension_a,
  CAST({{ dimension_b }} AS STRING) AS dimension_b,
  CAST({{ dimension_c }} AS STRING) AS dimension_c,
  {{ numerator }} AS numerator,
  {{ denominator }} AS denominator,
  {{ measure }} AS measure
FROM A
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

{%- endmacro -%}