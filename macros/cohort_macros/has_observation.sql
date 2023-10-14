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

{% macro has_observation(observation, code_system=None, lookback=None, patient_join_key= None, value_greater_than=None, value_less_than=None, return_all= FALSE) -%}
  {%- if return_all == TRUE %}
  (SELECT
  (SELECT AS STRUCT
      O.subject.patientid AS patient_id,
      O.encounter.encounterId, 
      LOWER('{{observation}}') AS observation_group,
      O.code.text AS observation_name,
      cc.code AS observation_code,
      {{ metric_date(['effective.datetime']) }} AS effective_date,
  ) AS observation_struct
  {%- else -%}
  EXISTS (
    SELECT
      O.subject.patientId
  {%- endif %}
  FROM {{ ref('Observation_view') }} AS O, UNNEST(code.coding) AS cc
  JOIN {{ ref('clinical_code_groups') }} AS L
    ON L.group = '{{observation}}'
    {%- if code_system != None %}
    AND L.system {{ sql_comparison_expression(code_system) }}
    {%- endif %}
    AND cc.system = L.system
    AND IF(L.match_type = 'exact', 
           cc.code = L.code,
            FALSE) # No support for other match types
  
  WHERE TRUE
  {%- if patient_join_key != None %}
    AND patient_join_key = O.subject.patientId
    {%- endif %}
  {%- if lookback != None %}
    AND {{ metric_date(['effective.datetime']) }} >= {{ get_snapshot_date() }} - INTERVAL {{ lookback }}
    {%- endif %}
    AND O.status NOT IN ('entered-in-error','cancelled')
    {%- if value_greater_than != None %}
    AND O.value.quantity.value > {{ value_greater_than }}
    {%- endif %}
    {%- if value_less_than != None %}
    AND O.value.quantity.value < {{ value_less_than }}
    {%- endif %}
  )
{%- endmacro %}