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

{% macro test_snake_case() %}

    {{ dbt_unittest.assert_equals(snake_case("abc"), "abc") }}
    {{ dbt_unittest.assert_equals(snake_case("Abc"), "abc") }}
    {{ dbt_unittest.assert_equals(snake_case("AbC"), "ab_c") }}
    {{ dbt_unittest.assert_equals(snake_case("foo_bar"), "foo_bar") }}
    {{ dbt_unittest.assert_equals(snake_case("FooBar"), "foo_bar") }}
    {{ dbt_unittest.assert_equals(snake_case("FoObAr"), "fo_ob_ar") }}

{% endmacro %}