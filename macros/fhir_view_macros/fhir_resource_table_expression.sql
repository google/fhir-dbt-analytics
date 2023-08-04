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

{%- macro fhir_resource_table_expression() -%}
    {%- if execute -%}
      {%- set fhir_resource = model_metadata(meta_key='fhir_resource') -%}
      {%- set fhir_resource_view = fhir_resource~"_view" -%}
      {%- set patient_reference_column = model_metadata(meta_key='patient_reference_column', model_name=fhir_resource_view) -%}
        SELECT * FROM {{ ref(fhir_resource_view) }}
    {%- else %}
        SELECT * FROM undefined
    {%- endif -%}
        {%- if var('cohort') != 'all_patients' and patient_reference_column != None %}
        WHERE EXISTS (
          SELECT cohort.patient_id
          FROM {{ ref(var('cohort')) }} AS cohort
          {%- if fhir_resource == 'Patient' %}
          WHERE id = cohort.patient_id
          {%- elif fhir_resource == 'Person' %}
          JOIN UNNEST(link) AS l ON l.target.patientId = cohort.patient_id
          {%- else %}
          WHERE {{patient_reference_column}}.patientId = cohort.patient_id
          {%- endif %}
        )
        {% endif -%}
{% endmacro %}