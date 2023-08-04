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

{%- macro metric_date(metric_date_columns, date_column_data_type) -%}
{%- if metric_date_columns == None %}
    CAST(NULL AS DATE)
{%- else -%}
    {%- if metric_date_columns | length > 1 -%}
      {%- set date_column = "COALESCE(" + metric_date_columns|join(", ") + ")" -%}
    {%- else %}
      {%- set date_column = metric_date_columns[0] -%}
    {%- endif -%}
      {{ local_date(date_column, date_column_data_type) }}
{%- endif -%}
{%- endmacro -%}