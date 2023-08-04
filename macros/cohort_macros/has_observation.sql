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

{% macro has_observation(observation, code_system=None, value_greater_than=None, value_less_than=None, lookback=None, patient_join_key= None) -%}
EXISTS (
  SELECT cc.code
  FROM {{ ref('Observation_view') }} AS O, UNNEST(code.coding) AS cc
  JOIN {{ ref('clinical_code_groups') }} AS L
    ON L.group = '{{observation}}'
    {%- if code_system != None %}
    AND L.system {{ sql_comparison_expression(code_system) }}
    {%- endif %}
    AND cc.system = L.system
    AND cc.code = L.code
    {%- if lookback != None %}
    AND DATE(O.effective.dateTime) >= {{ get_snapshot_date() }} - INTERVAL {{ lookback }}
    {%- endif %}
    {%- if value_greater_than != None %}
    AND O.value.quantity.value > {{ value_greater_than }}
    {%- endif %}
    {%- if value_less_than != None %}
    AND O.value.quantity.value < {{ value_less_than }}
    {%- endif %}
  {%- if patient_join_key != None %}
  WHERE 
    patient_join_key = E.subject.patientId
  {%- endif %}
)
{%- endmacro %}