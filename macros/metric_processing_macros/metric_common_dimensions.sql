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

{%- macro metric_common_dimensions(
  table_alias=None,
  include_metric_date=True,
  fhir_resource=None
) -%}

{%- if table_alias != None -%}
  {%- set prefix = table_alias ~ '.' -%}
{%- else -%}
  {%- set prefix = '' -%}
{%- endif -%}

{%- if include_metric_date %}
      {{ prefix ~ 'metric_date,' }}
{%- endif %}
      {{ site(table_alias, fhir_resource) }} AS site,
      {{ source_system(table_alias, fhir_resource) }} AS source_system,
      {{ prefix ~ 'fhir_mapping,' }}
{%- endmacro -%}