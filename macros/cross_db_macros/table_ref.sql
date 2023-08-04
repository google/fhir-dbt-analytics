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

{% macro table_ref(database, schema, table) -%}
  {{ return(adapter.dispatch('table_ref', 'fhir_dbt_analytics') (database, schema, table)) }}
{%- endmacro %}


{% macro default__table_ref(database, schema, table) -%}
  {{ return("`"~database~"`.`"~schema~"`.`"~table~"`") }}
{%- endmacro %}


{% macro spark__table_ref(database, schema, table) -%}
  {{ return(schema~"."~table) }}
{%- endmacro %}