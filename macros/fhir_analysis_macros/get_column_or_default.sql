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

{% macro get_column_or_default(column_name, fhir_resource=None, table_alias=None) %}

    {%- if execute and fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='primary_resource') -%}
    {%- endif -%}

    {%- if column_exists(column_name, fhir_resource) -%}
        {%- if table_alias != None -%}
          {{ return(table_alias ~ "." ~ column_name) }}
        {%- else -%}
          {{ return(column_name) }}
        {%- endif -%}
    {%- else -%}
        {{ return('NULL') }}
    {%- endif -%}

{% endmacro %}