{% macro condition_metadata(condition) -%}

  {{condition}}.master_patient_id,
  {{condition}}.cohort_name,
  {{condition}}.first_documented,
  {{condition}}.last_documented,
  {{condition}}.number_recorded

{%- endmacro %}