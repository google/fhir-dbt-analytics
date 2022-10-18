{%- macro create_dummy_table() -%}

{%- if execute -%}
{%- set patient_reference_column = model_metadata('patient_reference_column') -%}
{%- endif -%}

SELECT
  NULL AS id,
{%- if patient_reference_column == "link[].target" %}
  [STRUCT(STRUCT('no_data' AS patientId) AS target)] AS link,
{%- elif patient_reference_column != NULL %}
  STRUCT('no_data' AS patientId) AS {{patient_reference_column}},
{%- endif %}
  'no_data' AS fhir_mapping,
  CAST(NULL AS DATE) AS metric_date,
  'no_data' AS source_system,
  'no_data' AS site,
  'no_data' AS data_transfer_type

{%- endmacro -%}