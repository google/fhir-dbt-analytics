{{ config(
    meta = {
      "description": "Proportion of patients that are inactive",
      "short_description": "Inactive patients",
      "primary_resource": "Patient",
      "primary_fields": ['active'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Data distribution",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      CASE WHEN P.active = False THEN 1 ELSE 0 END AS patient_inactive
    FROM {{ ref('Patient') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(patient_inactive)',
    denominator = 'COUNT(id)'
) }}