{% macro condition_aggregate() -%}
SELECT
  C.condition.condition AS cohort_name,
  master_patient_id,
  MIN(C.condition.recorded_date) AS first_documented,
  MAX(C.condition.recorded_date) AS last_documented,
  COUNT(DISTINCT C.condition.recorded_date) AS number_recorded
FROM {{ ref('empi_patient_crosswalk') }} empi
JOIN cohort C
  ON C.condition.patient_id=empi.patient_id
GROUP BY 1, 2

{%- endmacro %}