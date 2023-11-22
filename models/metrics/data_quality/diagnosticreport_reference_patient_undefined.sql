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
      "description": "Proportion of DiagnosticReport resources that do not have a patient reference recorded",
      "short_description": "DiagRep ref. Patient - unrecorded",
      "primary_resource": "DiagnosticReport",
      "primary_fields": ['subject.patientId'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "DiagnosticReport.issued",
      "metric_date_description": "Diagnostic report latest version issue date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the diagnostic report (registered | partial | preliminary | final +)",
      "dimension_b": "category",
      "dimension_b_description": "The service category of the diagnostic report",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      {{ fhir_dbt_utils.code_from_codeableconcept(
        'category',
        'https://g.co/fhir/harmonized/diagnostic_report/category'
      ) }} AS category,
      {{ has_reference_value('subject', 'Patient') }} AS has_reference_value
    FROM {{ ref('DiagnosticReport') }} AS DiagnosticReport
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(1 - has_reference_value)',
    denominator = 'COUNT(id)'
) }}
