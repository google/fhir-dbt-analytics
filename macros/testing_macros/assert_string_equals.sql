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

{% macro assert_string_equals(x, y) %}
  {% set vars = {'pointer': ''} %}
  {% for c in x %}
    {% set pos = loop.index - 1 %}
    {% if y[pos] != c %}
      {{ exceptions.raise_compiler_error(
        "strings differ at character "~pos~" '"~c~"'<>'"~y[pos]~"'\n"
        ~x.replace("\n", "\\n")~" <>\n"~y.replace("\n", "\\n")~"\n"~vars.pointer~"^") }}
    {% endif %}
    # Add two spaces for \n, one space otherwise.
    {{ vars.update({ 'pointer': vars.pointer ~ ('  ' if c == '\n' else ' ') }) }}
  {% endfor %}
{% endmacro %}

