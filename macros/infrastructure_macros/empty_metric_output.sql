{%- macro empty_metric_output() %}
SELECT
  CURRENT_DATETIME() as execution_datetime,
  '{{this.name}}' AS metric_name,
  CAST(NULL AS STRING) AS fhir_mapping,
  '{{var('source_system_default')}}' AS source_system,
  '{{var('data_transfer_type_default')}}' AS data_transfer_type,
  CAST(NULL AS DATE) AS metric_date,
  '{{var('site_default')}}' AS site,
  CAST(NULL AS STRING) AS slice_a,
  CAST(NULL AS STRING) AS slice_b,
  CAST(NULL AS STRING) AS slice_c,
  CAST(NULL AS INT64) AS numerator,
  CAST(NULL AS INT64) AS denominator_cohort,
  CAST(NULL AS FLOAT64) AS measure
{%- endmacro -%}