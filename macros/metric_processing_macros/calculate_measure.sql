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

{% macro calculate_measure(metric_definition_alias="D", metric_alias="M") -%}
   CASE
    WHEN {{metric_definition_alias}}.calculation = 'COUNT'
    THEN SUM({{metric_alias}}.measure)
    WHEN {{metric_definition_alias}}.calculation IN ('PROPORTION', 'RATIO')
    THEN SUM({{metric_alias}}.numerator)/NULLIF(SUM({{metric_alias}}.denominator),0)
    WHEN {{metric_definition_alias}}.calculation = 'DISTRIBUTION'
    THEN NULL
    ELSE {{ error(concat(["'Unknown calculation:'", metric_definition_alias~".calculation"])) }}
   END
{%- endmacro %}