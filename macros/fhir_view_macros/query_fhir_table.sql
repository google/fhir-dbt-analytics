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

{%- macro query_fhir_table(fhir_resource) -%}
    {%- set metric_date_columns = get_metric_date_columns() -%}
    {%- set date_column_data_type = column_data_type(metric_date_columns[0], get_source_table_name(fhir_resource)) %}
SELECT
    *,
    CAST(NULL AS STRING) AS fhir_mapping,
    {{- metric_date(metric_date_columns, date_column_data_type) }} AS metric_date,
    {{- metric_hour(metric_date_columns, date_column_data_type) }} AS metric_hour,
    {{- metadata_cols() -}}
FROM {{ source('fhir', get_source_table_name(fhir_resource)) }}
{% endmacro %}