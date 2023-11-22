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


{% macro has_medication_request(medication, code_system=None, lookback=None, patient_join_key= None, return_all= False) -%}
  {%- if return_all == True %}
  (SELECT
  (SELECT AS STRUCT
      MR.id,
      MR.subject.patientid AS patient_id, 
      MR.encounter.encounterId AS encounter_id,
      LOWER(L.group_type) AS clinical_group_type, 
      LOWER(L.group) AS clinical_group_name,
      {{ get_medication('text') }} AS free_text_name,
      {{ get_medication('code','L.system') }} AS code,
      {{ fhir_dbt_utils.metric_date(['authoredOn']) }} AS clinical_date,
  ) AS summary_struct
  {%- else -%}
  EXISTS (
    SELECT
      MR.subject.patientId
  {%- endif %}
  FROM {{ ref('MedicationRequest') }} AS MR
  LEFT JOIN {{ ref('Medication') }} AS m
    ON m.id = medication.reference.medicationid
  JOIN {{ ref('clinical_code_groups') }} AS L
    ON L.group  {{ sql_comparison_expression(medication) }}
    {%- if code_system != None %}
    AND L.system {{ sql_comparison_expression(code_system) }}
    {%- endif %}
  WHERE IF(L.match_type = 'exact',
           {{ get_medication('code','L.system') }} = L.code,
            FALSE) # No support for other match types
  {%- if patient_join_key != None %}
    AND patient_join_key = MR.subject.patientId
    {%- endif %}
  {%- if lookback != None %}
    AND {{ fhir_dbt_utils.metric_date(['authored_on']) }} >= {{ fhir_dbt_utils.get_snapshot_date() }} - INTERVAL {{ lookback }}
    {%- endif %}
    AND MR.status NOT IN ('entered-in-error','cancelled','draft')
  )
{%- endmacro %}