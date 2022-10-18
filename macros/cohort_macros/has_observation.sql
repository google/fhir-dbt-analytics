{% macro has_observation(observation, code_system=None, value_greater_than=None, value_less_than=None, lookback=None) -%}
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
  WHERE P.id = O.subject.patientId
)
{%- endmacro %}