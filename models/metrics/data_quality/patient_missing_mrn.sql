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
      {%- if column_exists('identifier.type') %}
      (
        SELECT 1 - SIGN(COUNT(*))
        FROM {{ spark_parenthesis(
          unnest_multiple([array_config('identifier', 'i'), array_config('i.type.coding', 't')])
        )}}
        WHERE i.value IS NOT NULL AND i.value <> ''
          AND t.system = 'http://terminology.hl7.org/CodeSystem/v2-0203'
          AND t.code='MR'
      )
      AS patient_missing_mrn
      {%- else %}
      1 AS patient_missing_mrn
      {%- endif %}
    FROM {{ ref('Patient') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(patient_missing_mrn)',
    denominator = 'COUNT(id)'
) }}
