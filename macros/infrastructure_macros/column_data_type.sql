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

{% macro column_data_type(column_name, source_table) %}

    {%- set relation = adapter.get_relation(
          database = var('database'),
          schema = var('schema'),
          identifier = source_table
        )
    -%}

    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {%- for top_level_column in columns -%}
        {% if column_name == top_level_column.name %}
            {% do return (top_level_column.data_type) %}
        {% endif %}
        {% for column in flatten_column(top_level_column) %}
            {% if column_name == column.name %}
                {% do return (column.data_type) %}
            {% endif %}
        {%- endfor -%}
    {%- endfor -%}

{% endmacro %}
