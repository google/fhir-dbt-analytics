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

{% macro sql_comparison_expression(code_string_or_list) -%}
 {%- if code_string_or_list is string -%}
  = '{{ code_string_or_list }}'
  {%- elif code_string_or_list is iterable -%}
  IN ('{{ code_string_or_list|join("', '") }}')
  {%- else -%}
  {{ exceptions.raise_compiler_error("Invalid argument " ~ code_string_or_list) }}
  {%- endif -%}
{% endmacro %}