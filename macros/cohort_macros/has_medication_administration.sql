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

{% macro has_medication_administration(medication, code_system=None, lookback=None, patient_join_key= None, return_all= FALSE) -%}
  {%- if return_all == TRUE %}
  (SELECT
  (SELECT AS STRUCT
      MA.subject.patientid AS patient_id, 
      LOWER('{{medication}}') AS medication_group,
      {{ get_medication('text') }} AS medication_free_text_name,
      {{ get_medication('code',code_system) }} AS medication_code,
      #(SELECT cc.code FROM UNNEST(get_medication().coding) AS cc WHERE cc.system=L.system ) AS medication_code,
      {{ metric_date(['effective.period.start']) }} AS administered_date,
  ) AS medication_administration_struct
  {%- else -%}
  EXISTS (
    SELECT
      MA.subject.patientId
  {%- endif %}
  FROM {{ ref('MedicationAdministration_view') }} AS MA
  JOIN {{ ref('clinical_code_groups') }} AS L
    ON L.group = '{{medication}}'
    {%- if code_system != None %}
    AND L.system {{ sql_comparison_expression(code_system) }}
    {%- endif %}
    AND IF(L.match_type = 'exact', 
           {{ get_medication('code',code_system)}}  = L.code,
            FALSE) # No support for other match types

  WHERE TRUE
  {%- if patient_join_key != None %}
    AND patient_join_key = MA.subject.patientId
    {%- endif %}
  {%- if lookback != None %}
    AND {{ metric_date(['effective.period.start']) }} >= {{ get_snapshot_date() }} - INTERVAL {{ lookback }}
    {%- endif %}
    AND MA.status NOT IN ('entered-in-error','not-done')
  )
{%- endmacro %}