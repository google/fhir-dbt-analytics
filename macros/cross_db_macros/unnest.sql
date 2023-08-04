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

{% macro unnest(array, alias = "") -%}
  {{ return (adapter.dispatch('unnest', 'fhir_dbt_analytics') (array, alias)) }}
{%- endmacro %}


{% macro default__unnest(array, alias) -%}
  UNNEST({{ array }}){% if alias|length > 0 %} {{ alias }}{% endif %}
{%- endmacro -%}


{# Select the field as `ac`: this "cheeky select" trick allows us to write the unnest as one
    statement. #}
{% macro spark__unnest(array, alias) -%}
  SELECT EXPLODE(ac){% if alias|length > 0 %} AS {{ alias }}{% endif %} FROM (SELECT {{ array }} AS ac)
{%- endmacro %}
