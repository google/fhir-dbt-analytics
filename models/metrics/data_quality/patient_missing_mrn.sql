{{ config(
    meta = {
      "description": "Proportion of patients that have missing MRN",
      "short_description": "Patients missing MRN",
      "primary_resource": "Patient",
      "primary_fields": ['identifier.value'],
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
      CASE WHEN 'http://terminology.hl7.org/CodeSystem/v2-0203' IN (
        SELECT t.system
        FROM UNNEST(identifier) i, UNNEST(i.type.coding) t
        WHERE i.value IS NOT NULL AND i.value <> ''
      )
      THEN 0 ELSE 1 END AS patient_missing_mrn
    FROM {{ ref('Patient') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(patient_missing_mrn)',
    denominator = 'COUNT(id)'
) }}