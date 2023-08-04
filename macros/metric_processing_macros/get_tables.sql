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

{% macro get_tables(table_name_wildcard="%") %}
  SELECT
    T.table_name,
    CONCAT('`', T.table_catalog, '`.`', T.table_schema, '`.`', T.table_name, '`') AS fully_qualified_bq_table
  FROM {{target.project}}.{{target.dataset}}.INFORMATION_SCHEMA.TABLES AS T
  WHERE T.table_name LIKE '{{ table_name_wildcard }}'
{%- endmacro %}