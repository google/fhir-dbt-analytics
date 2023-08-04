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

{%- macro metric_common_dimensions(table_alias=None, exclude_col=None) -%}

{%- set exclude_cols = [exclude_col] -%}
{%- set common_dimensions = [
  'fhir_mapping',
  'source_system',
  'data_transfer_type',
  'metric_date',
  'site'
] -%}
{%- set columns = common_dimensions | reject('in', exclude_cols) -%}

{%- if table_alias != None -%}
  {%- set prefix = table_alias ~ '.' -%}
{%- else -%}
  {%- set prefix = '' -%}
{%- endif -%}

{%- for col in columns %}
  {{ prefix ~ col }},
{%- endfor -%}

{%- endmacro -%}