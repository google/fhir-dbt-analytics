{% macro resource_aggregate(encounter_level_aggregate=False, quantity_summary=False) -%}
 
SELECT
  {% if encounter_level_aggregate == True +%}
  summary_struct.encounter_id,
  {%- else %}
  CAST(NULL AS STRING) AS encounter_id,
  {%- endif -%}
  C.summary_struct.clinical_group_name AS group_name,
  C.summary_struct.clinical_group_type AS group_type,
  master_patient_id,
  {%- if quantity_summary == True %}
  summary_struct.unit,
  APPROX_QUANTILES(summary_struct.result, 100)[OFFSET(50)] AS median_documented,
  APPROX_QUANTILES(summary_struct.result, 100)[OFFSET(90)] AS pct90_documented,
  APPROX_QUANTILES(summary_struct.result, 100)[OFFSET(95)] AS pct95_documented,
  APPROX_QUANTILES(summary_struct.result, 100)[OFFSET(99)] AS pct99_documented,
  AVG(summary_struct.result) as avg_documented,
  STDDEV(summary_struct.result) as stddev_documented,
  {%- endif %}
  MIN(C.summary_struct.clinical_date) AS first_documented,
  MAX(C.summary_struct.clinical_date) AS last_documented,
  COUNT(DISTINCT C.summary_struct.id) AS number_documented
FROM {{ ref('empi_patient_crosswalk') }} empi
JOIN cohort C
  ON C.summary_struct.patient_id=empi.patient_id 
{%- if quantity_summary == True %}
  GROUP BY 1, 2, 3, 4, 5
{%- else %}
  GROUP BY 1, 2, 3, 4
{%- endif %}
{%- endmacro %}