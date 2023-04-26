{% macro has_encounter(class=None, status=None, lookback=None, patient_join_key=None, return_all= FALSE) -%}
{%- if return_all == TRUE %}
(SELECT
  (SELECT AS STRUCT
    E.id,
    E.subject.patientid,
    ({{ code_from_codeableconcept('e.hospitalization.dischargeDisposition', 'http://terminology.hl7.org/CodeSystem/discharge-disposition') }}) AS discharge_disposition,
    {{ metric_date(['period.start']) }} AS start_date,
    {{ metric_date(['period.end']) }} AS end_date,
    e.status,
    e.class.code AS class,
    {%- if column_exists('serviceType') %}
    e.serviceType.text AS service,
    {%- else %}
    CAST(NULL AS STRING) AS service,
    {%- endif %}
    {%- if column_exists('classHistory') %}
    IF('EMER' IN (SELECT class.code FROM UNNEST(classHistory)) OR class.code = 'EMER', TRUE, FALSE)
    AS  emergency_adm_flag,
    {%- else %}
    IF(class.code = 'EMER', TRUE, FALSE) AS  emergency_adm_flag,
    {%- endif %}
    IF( e.period.end IS NOT NULL
        AND {{ metric_date(['period.end']) }} < {{ get_snapshot_date() }}
        AND e.period.end <> '',
        DATE_DIFF(
          {{ metric_date(['period.end']) }},
          {{ metric_date(['period.start']) }},
          DAY), NULL
    ) AS LOS
  ) AS encounter
    {%- else -%}
  EXISTS (
    SELECT
      E.subject.patientId
  {%- endif %}
  FROM {{ ref('Encounter_view') }} AS E
  
  WHERE 0=0
  {%- if patient_join_key != None %}
  AND patient_join_key = E.subject.patientId
  {%- endif %}
  {%- if status != None %}
  AND E.status {{ sql_comparison_expression(status) }}
  {%- endif %}
  AND e.period.start IS NOT NULL
  AND e.period.start <> ''
  {%- if class != None %}
  AND class.code {{ sql_comparison_expression(class) }}
  {%- endif %}
  AND DATE(E.period.start) <= {{ get_snapshot_date() }}
  {%- if lookback != None %}
  AND DATE(E.period.start) >= {{ get_snapshot_date() }} - INTERVAL {{ lookback }}
  {%- endif %}
)
{%- endmacro %}