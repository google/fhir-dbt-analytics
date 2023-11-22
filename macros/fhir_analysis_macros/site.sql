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

{%- macro site(table_alias=None, fhir_resource=None) -%}

  {%- if table_alias != None -%}
    {%- set prefix = table_alias ~ '.' -%}
  {%- else -%}
    {%- set prefix = '' -%}
  {%- endif -%}

    {#- A FHIR dataset can often be populated from data originating from different sites -#}
    {#- Update code here to extract site from where this is recorded in your FHIR data -#}
    '{{ var('site_default') }}'

{%- endmacro -%}