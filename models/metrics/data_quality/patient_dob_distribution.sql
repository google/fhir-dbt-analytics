{{ config(
    meta = {
      "description": "Patient birthdate distribution",
      "short_description": "Patient dob distribution",
      "primary_resource": "Patient",
      "primary_fields": ['birthdate'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Data distribution",
      "dimension_a": "decade",
      "dimension_a_description": "The patient's birth decade'",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      CAST(ROUND(SAFE_CAST(SUBSTR(P.birthdate,0,4) AS {{ type_long() }}),-1) AS STRING) AS decade,
    FROM {{ ref('Patient') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql
) }}