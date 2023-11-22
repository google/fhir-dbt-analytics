{#
/* Copyright 2022 Google LLC

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
      "description": "Proportion of patients without structured name",
      "short_description": "Patients missing struct name",
      "primary_resource": "Patient",
      "primary_fields": ['name.family', 'name.given'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Data completeness",
      "dimension_a": "active",
      "dimension_a_description": "Whether this patient record is in active use",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      CAST({{ get_column_or_default('active') }} AS STRING) AS active,
      CASE WHEN P.name IS NULL OR ARRAY_LENGTH(P.name) = 0 OR {{ fhir_dbt_utils.safe_offset("P.name", 0) }}.family IS NULL OR {{ fhir_dbt_utils.safe_offset("P.name", 0) }}.family = '' OR {{ fhir_dbt_utils.safe_offset("P.name", 0) }}.given IS NULL OR ARRAY_LENGTH({{ fhir_dbt_utils.safe_offset("P.name", 0) }}.given) = 0 OR {{ fhir_dbt_utils.safe_offset("P.name", 0) }}{{ fhir_dbt_utils.safe_offset(".given", 0) }} = '' THEN 1 ELSE 0 END AS patient_structured_name_missing

    FROM {{ ref('Patient') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(patient_structured_name_missing)',
    denominator = 'COUNT(id)'
) }}