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

{%- macro create_dummy_table() -%}

{%- if execute -%}
{%- set patient_reference_column = model_metadata('patient_reference_column') -%}
{%- endif -%}

SELECT
  CAST(NULL AS STRING) AS id,
{%- if patient_reference_column == "link[].target" %}
  {{ dbt.array_construct(["STRUCT(STRUCT('no_data' AS patientId) AS target)"]) }} AS link,
{%- elif patient_reference_column != None %}
  STRUCT('no_data' AS patientId) AS {{patient_reference_column}},
{%- endif %}
  CAST(NULL AS STRING) AS fhir_mapping,
  CAST(NULL AS DATE) AS metric_date,
  CAST(NULL AS TIMESTAMP) AS metric_hour,
  '{{ var('source_system_default') }}' AS source_system,
  '{{ var('site_default') }}' AS site,
  '{{ var('data_transfer_type_default') }}' AS data_transfer_type

{%- endmacro -%}