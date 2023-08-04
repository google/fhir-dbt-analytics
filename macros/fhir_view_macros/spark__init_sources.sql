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

{% macro spark__init_sources(fhir_resources, parquet_location) %}
  {% set sql = "CREATE DATABASE IF NOT EXISTS "~var('schema') %}
  {{ print(sql) }}
  {{ run_query(sql) }}

  {% for fhir_resource in fhir_resources.split(",") %}
    {% set location = parquet_location~"/"~(fhir_resource|trim|lower) %}
    {% set sql = "CREATE TABLE IF NOT EXISTS "~var('schema')~"."~(fhir_resource|trim)
                ~" USING PARQUET LOCATION '"~location~"'" %}
    {{ print(sql) }}
    {{ run_query(sql) }}
  {% endfor %}
{% endmacro %}
