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
      "description": "Proportion of patients that are flagged as test patients",
      "short_description": "Test patients",
      "primary_resource": "Patient",
      "primary_fields": ['meta.security.system','meta.security.code'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Data completeness",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      (
        SELECT SIGN(COUNT(*)) FROM {{ spark_parenthesis(unnest('P.meta.security', 's')) }}
        WHERE s.system = 'http://terminology.hl7.org/CodeSystem/v3-ActReason'
          AND s.code = 'HTEST'
      )
      AS test_patient
    FROM {{ ref('Patient') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(test_patient)',
    denominator = 'COUNT(id)'
) }}