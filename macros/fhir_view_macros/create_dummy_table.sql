{%- macro create_dummy_table() -%}

{%- if execute -%}
{%- set patient_reference_column = model_metadata('patient_reference_column') -%}
{%- endif -%}

SELECT
  CAST(NULL AS STRING) AS id,
{%- if patient_reference_column == "link[].target" %}
  [STRUCT(STRUCT('no_data' AS patientId) AS target)] AS link,
{%- elif patient_reference_column != None %}
  STRUCT('no_data' AS patientId) AS {{patient_reference_column}},
{%- endif %}
  CAST(NULL AS STRING) AS fhir_mapping,
  CAST(NULL AS DATE) AS metric_date,
  '{{ var('source_system_default') }}' AS source_system,
  '{{ var('site_default') }}' AS site,
  '{{ var('data_transfer_type_default') }}' AS data_transfer_type

{%- endmacro -%}