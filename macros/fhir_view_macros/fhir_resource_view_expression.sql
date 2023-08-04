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

{%- macro fhir_resource_view_expression() -%}

{%- if execute -%}
    {%- set fhir_resource = model_metadata(meta_key='fhir_resource') -%}
{%- else %}
    {%- set fhir_resource = 'N/A' -%}
{%- endif -%}

{%- if fhir_resource_exists(fhir_resource) -%}
{{ query_fhir_table(fhir_resource) }}
{%- else %}
{{ create_dummy_table() }}
{%- endif -%}

{%- endmacro -%}