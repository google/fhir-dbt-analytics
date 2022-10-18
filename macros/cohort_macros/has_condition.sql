{% macro has_condition(condition, code_system=None, lookback=None) -%}
 EXISTS (
  SELECT cc.code
  FROM {{ ref('Condition_view') }} AS C, UNNEST(code.coding) AS cc
  JOIN {{ ref('clinical_code_groups') }} AS L
    ON L.group = '{{condition}}'
    {%- if code_system != None %}
    AND L.system {{ sql_comparison_expression(code_system) }}
    {%- endif %}
    AND cc.system = L.system
    AND cc.code = L.code
    {%- if lookback != None %}
    AND DATE(C.recordedDate) >= {{ get_snapshot_date() }} - INTERVAL {{ lookback }}
    {%- endif %}
  WHERE P.id = C.subject.patientId
)
{%- endmacro %}