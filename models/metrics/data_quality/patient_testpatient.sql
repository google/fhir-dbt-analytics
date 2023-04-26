{{ config(
    meta = {
      "description": "Proportion of patients that are flagged as test patients",
      "short_description": "Test patients",
      "primary_resource": "Patient",
      "primary_fields": ['meta.security.system','meta.security.code'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Data completeness",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      CASE WHEN 'HTEST' IN 
              (SELECT code FROM UNNEST(P.meta.security) 
               WHERE system = 'http://terminology.hl7.org/CodeSystem/v3-ActReason') 
           THEN  1 ELSE 0 END AS test_patient
    FROM {{ ref('Patient') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(test_patient)',
    denominator = 'COUNT(id)'
) }}