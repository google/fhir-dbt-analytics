{% macro has_condition(condition, code_system=None, lookback=None, patient_join_key= None, return_all= FALSE) -%}
  {%- if return_all == TRUE %}
  (SELECT
  (SELECT AS STRUCT
      C.subject.patientid AS patient_id, 
      LOWER('{{condition}}') AS condition,
      cc.code AS code,
      {{ metric_date(['recordedDate']) }} AS recorded_date,
      {%- if column_exists('onset.dateTime') %}
      {{ metric_date(['onset.dateTime']) }} AS onset_date
      {%- else -%}
      (NULL) AS onset_date
      {%- endif %}   
  ) AS condition
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
           REPLACE(cc.code,'.','') LIKE CONCAT('\"', L.code, '%\"'),
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