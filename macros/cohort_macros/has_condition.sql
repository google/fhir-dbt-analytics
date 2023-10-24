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

{% macro has_condition(condition, code_system=None, lookback=None, patient_join_key= None, return_all= False) -%}
  {%- if return_all == True %}
  (SELECT
  (SELECT AS STRUCT
      C.id,
      C.subject.patientid AS patient_id,
      LOWER(L.group_type) AS clinical_group_type, 
      LOWER(L.group) AS clinical_group_name,
      cc.code AS code,
      {{ metric_date(['recordedDate']) }} AS clinical_date,
      {%- if column_exists('onset.dateTime') %}
      {{ metric_date(['onset.dateTime']) }} AS onset_date
      {%- else -%}
      (NULL) AS onset_date
      {%- endif %}   
  ) AS summary_struct
  {%- else -%}
  EXISTS (
    SELECT
      C.subject.patientId
  {%- endif %}
  FROM {{ ref('Condition_view') }} AS C, UNNEST(code.coding) AS cc
  JOIN {{ ref('clinical_code_groups') }} AS L
    ON L.group = '{{condition}}'
    {%- if code_system != None %}
    AND L.system {{ sql_comparison_expression(code_system) }}
    {%- endif %}
    AND cc.system = L.system
    AND IF(L.match_type = 'start',
           CONCAT('\"',REPLACE(cc.code,'.',''),'\"') LIKE CONCAT('\"', L.code, '%\"'),
           REPLACE(cc.code,'.','') = L.code)
  
  WHERE 0=0
  {%- if patient_join_key != None %}
    AND patient_join_key = C.subject.patientId
  {%- endif %}
  {%- if lookback != None %}
    AND DATE(C.recordedDate) >= {{ get_snapshot_date() }} - INTERVAL {{ lookback }}
  {%- endif %}
    AND C.verificationStatus IS NULL
        OR
          {{ code_from_codeableconcept(
          'verificationStatus',
          'http://terminology.hl7.org/CodeSystem/condition-ver-status',
          index=0
          ) }} NOT IN ('entered-in-error')
  )
{%- endmacro %}