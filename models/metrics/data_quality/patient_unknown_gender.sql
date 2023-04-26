{{ config(
    meta = {
      "description": "Proportion of patients that have unknown gender",
      "short_description": "Patients unknown gender",
      "primary_resource": "Patient",
      "primary_fields": ['gender'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Data completeness",
      "dimension_a": "active",
      "dimension_a_description": "The patient active status  (true | false)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      CAST({{ get_column_or_default('active') }} AS STRING) AS active,
      CASE WHEN LOWER(P.gender) = 'unknown' THEN 1 ELSE 0 END AS patient_gender_unknown
    FROM {{ ref('Patient') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(patient_gender_unknown)',
    denominator = 'COUNT(id)'
) }}