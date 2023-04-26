{% macro has_procedure(procedure, code_system=None, lookback=None,  patient_join_key= None) -%}
 EXISTS (
  SELECT cc.code
  FROM {{ ref('Procedure_view') }} AS Pr, UNNEST(code.coding) AS cc
  JOIN {{ ref('clinical_code_groups') }} AS L
    ON L.group = '{{procedure}}'
    {%- if code_system != None %}
    AND L.system {{ sql_comparison_expression(code_system) }}
    {%- endif %}
    AND cc.system = L.system
    AND cc.code = L.code
    {%- if lookback != None %}
    AND DATE(COALESCE(Pr.performed.dateTime, Pr.performed.period.start)) >= {{ get_snapshot_date() }} - INTERVAL {{ lookback }}
    {%- endif %}
  {%- if patient_join_key != None %}
  WHERE 
    patient_join_key = E.subject.patientId
  {%- endif %}
)
{%- endmacro %}