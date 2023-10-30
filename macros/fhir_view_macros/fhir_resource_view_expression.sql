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
{%- else -%}
    {%- set fhir_resource = 'N/A' -%}
{%- endif -%}

{%- if var('snake_case_fhir_tables') -%}
    {%- set fhir_table = snake_case(fhir_resource) -%}
{%- else -%}
    {% set fhir_table = fhir_resource %}
{% endif -%}

{%- if target.name == "internal_pipeline"  -%}
    {%- set metric_date_columns = get_metric_date_columns() -%}
    {%- set date_column_data_type = 'STRING' -%}
SELECT
    *,
    {%- if fhir_resource not in ('Binary') %}
    meta.mappingId AS fhir_mapping,
    {%- else %}
    NULL AS fhir_mapping,
    {%- endif %}
    {{ metric_date(metric_date_columns, date_column_data_type) }} AS metric_date,
    {{ metric_hour(metric_date_columns, date_column_data_type) }} AS metric_hour,
    {{- metadata_cols() -}}
FROM {{fhir_table}}
{% elif not fhir_resource_exists(fhir_resource) %}
    {{ create_dummy_table() }}
{%- elif var('multiple_tables_per_resource') -%}
    {% set maps = get_maps_for_resource(fhir_resource) %}
    {{ return(build_union_query(maps)) }}
{%- else %}
    {%- set metric_date_columns = get_metric_date_columns() -%}
    {%- set date_column_data_type = column_data_type(metric_date_columns[0], fhir_table) %}
SELECT
    *,
    CAST(NULL AS STRING) AS fhir_mapping,
    {{ metric_date(metric_date_columns, date_column_data_type) }} AS metric_date,
    {{ metric_hour(metric_date_columns, date_column_data_type) }} AS metric_hour,
    {{- metadata_cols() -}}
FROM {{ source('fhir', fhir_table) }}
{%- endif -%}

{%- endmacro -%}