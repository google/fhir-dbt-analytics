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

{% macro test_select_from_unnest() -%}
  {{ return(adapter.dispatch('test_select_from_unnest', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}


{% macro default__test_select_from_unnest() -%}
  {{ assert_string_equals(
      select_from_unnest("c.code", "UNNEST(my_array) c", where="c.value > 1", order_by="c.key"),
      "SELECT c.code FROM UNNEST(my_array) c WHERE c.value > 1 ORDER BY c.key LIMIT 1") }}
{%- endmacro %}


{% macro spark__test_select_from_unnest() -%}
  {{ assert_string_equals(
      spark__select_from_unnest(
        "c.code",
        "SELECT EXPLODE(ac) AS c FROM (SELECT my_array AS ac)",
        where="c.value > 1",
        order_by="c.key"),
  "SELECT ELEMENT_AT(COLLECT_LIST(c.code), 1) FROM (
  SELECT * FROM (SELECT EXPLODE(ac) AS c FROM (SELECT my_array AS ac))
  WHERE c.value > 1
  ORDER BY c.key)") }}
{% endmacro %}
