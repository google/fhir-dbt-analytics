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

{% macro select_from_unnest(select, unnested, where="1=1", order_by="1") -%}
  {{ return (adapter.dispatch('select_from_unnest', 'fhir_dbt_analytics') (select, unnested, where, order_by)) }}
{%- endmacro %}


{% macro default__select_from_unnest(select, unnested, where, order_by) -%}
SELECT {{ select }} FROM {{ unnested }} WHERE {{ where }} ORDER BY {{ order_by }} LIMIT 1
{%- endmacro %}


{% macro spark__select_from_unnest(select, unnested, where, order_by) -%}
SELECT ELEMENT_AT(COLLECT_LIST({{ select }}), 1) FROM (
  SELECT * FROM ({{ unnested }})
  WHERE {{ where }}
  ORDER BY {{ order_by }})
{%- endmacro %}
