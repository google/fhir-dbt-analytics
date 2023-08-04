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

{% macro test_code_from_codeableconcept() -%}
  {{ return(adapter.dispatch('test_code_from_codeableconcept', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}


{% macro default__test_code_from_codeableconcept() -%}
  {{ assert_string_equals(
    _code_from_codeableconcept(
        field_name="value",
        code_system="code_system_foo",
        field_is_array=False,
        null_values=[''],
        return_field="code"),
    "(SELECT c.code FROM UNNEST(value.coding) c WHERE c.system = 'code_system_foo' ORDER BY c.code LIMIT 1)") }}

  {{ assert_string_equals(
    _code_from_codeableconcept(
        field_name="value",
        code_system="code_system_foo",
        field_is_array=True,
        null_values=[''],
        return_field="code"),
    "(SELECT c.code FROM UNNEST(value) f, UNNEST(f.coding) c WHERE c.system = 'code_system_foo' ORDER BY c.code LIMIT 1)") }}

  {{ assert_string_equals(
    _code_from_codeableconcept(
        field_name="value",
        code_system="code_system_foo",
        field_is_array=True,
        null_values=[''],
        return_field="code",
        return_int=True),
    "(
        SELECT SIGN(COUNT(*))
        FROM UNNEST(value) f, UNNEST(f.coding) c
        WHERE c.system = 'code_system_foo'
        AND c.code IS NOT NULL
        AND c.code NOT IN ('')
      )") }}
{%- endmacro %}


{% macro spark__test_code_from_codeableconcept() -%}
  {# Consider adding Spark tests. But maybe lower priority, because the individual pieces are
     tested already #}
{%- endmacro %}
