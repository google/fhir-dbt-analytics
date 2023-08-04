{#
/* Copyright 2023 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#}
{{ config(
    meta = {
      "description": "Proportion of patients that have missing birthdate",
      "short_description": "Patients missing dob",
      "primary_resource": "Patient",
      "primary_fields": ['birthdate'],
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
      CASE WHEN P.birthdate IS NULL OR  P.birthdate = '' THEN 1 ELSE 0 END AS patient_missing_dob
    FROM {{ ref('Patient') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(patient_missing_dob)',
    denominator = 'COUNT(id)'
) }}