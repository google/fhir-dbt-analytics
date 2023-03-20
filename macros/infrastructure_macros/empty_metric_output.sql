{%- macro empty_metric_output() %}
SELECT
  {{ current_datetime() }} as execution_datetime,
  '{{this.name}}' AS metric_name,
  CAST(NULL AS STRING) AS fhir_mapping,
  '{{var('source_system_default')}}' AS source_system,
  '{{var('data_transfer_type_default')}}' AS data_transfer_type,
  CAST(NULL AS DATE) AS metric_date,
  '{{var('site_default')}}' AS site,
  CAST(NULL AS STRING) AS dimension_a,
  CAST(NULL AS STRING) AS dimension_b,
  CAST(NULL AS STRING) AS dimension_c,
  CAST(NULL AS {{ type_long() }}) AS numerator,
  CAST(NULL AS {{ type_long() }}) AS denominator,
  CAST(NULL AS {{ type_double() }}) AS measure
{%- endmacro -%}