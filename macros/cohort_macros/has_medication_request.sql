{% macro has_medication_request(medication, code_system=None, lookback=None) -%}
{%- set snapshot_date = get_snapshot_date() -%}
 EXISTS (
  SELECT cc.code
  FROM {{ ref('MedicationRequest_view') }} AS M, UNNEST(medication.codeableConcept.coding) AS cc
  JOIN {{ ref('clinical_code_groups') }} AS L
    ON L.group = '{{medication}}'
    {%- if code_system != None %}
    AND L.system {{ sql_comparison_expression(code_system) }}
    {%- endif %}
    AND cc.system = L.system
    AND cc.code = L.code
    {%- if lookback != None %}
    AND DATE(M.authoredOn) >= {{ get_snapshot_date() }} - INTERVAL {{ lookback }}
    {%- endif %}
  WHERE P.id = M.subject.patientId
)
{%- endmacro %}