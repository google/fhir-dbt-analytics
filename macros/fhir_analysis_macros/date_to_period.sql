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

{% macro date_to_period(date_column, date_period) -%}
  {{ return(adapter.dispatch('date_to_period', 'fhir_dbt_analytics') (date_column, date_period)) }}
{%- endmacro %}


{% macro default__date_to_period(date_column, date_period) -%}
{%- if date_period == 'YEAR' -%}
 {%- set format_string = '%Y' -%}
{%- elif date_period == 'QUARTER' -%}
 {%- set format_string  = '%Y Q%Q' -%}
{%- elif date_period == 'MONTH' -%}
 {%- set format_string = '%Y M%m' -%}
{%- else -%}
 {%- set format_string = '%F' -%}
{%- endif -%}
FORMAT_DATE('{{format_string}}', {{date_column}})
{%- endmacro %}


{% macro spark__date_to_period(date_column, date_period) -%}
{%- if date_period == 'YEAR' -%}
 {%- set format_string = 'y' -%}
{%- elif date_period == 'QUARTER' -%}
 {%- set format_string  = "y qqq" -%}
{%- elif date_period == 'MONTH' -%}
 {%- set format_string = "y 'M'M" -%}
{%- else -%}
 {%- set format_string = 'yMMdd' -%}
{%- endif -%}
DATE_FORMAT({{date_column}}, '{{format_string}}')
{%- endmacro %}
