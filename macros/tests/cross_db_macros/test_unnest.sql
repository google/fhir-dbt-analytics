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

{% macro test_unnest() -%}
  {{ return(adapter.dispatch('test_unnest', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}


{% macro default__test_unnest() -%}
  {{ assert_string_equals(
      unnest('my_array', 'c' ),
      "UNNEST(my_array) c") }}

  {{ assert_string_equals(
      unnest('my_array' ),
      "UNNEST(my_array)") }}
{%- endmacro %}


{% macro spark__test_unnest() -%}
  {{ assert_string_equals(
        spark__unnest('my_array', 'c' ),
        "SELECT EXPLODE(ac) AS c FROM (SELECT my_array AS ac)") }}
  {{ assert_string_equals(
        spark__unnest('my_array'),
        "SELECT EXPLODE(ac) FROM (SELECT my_array AS ac)") }}
{%- endmacro %}
