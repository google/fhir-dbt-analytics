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
      "description": "Proportion of Condition resources that contain a non-empty code.text",
      "short_description": "Condition with code.text",
      "primary_resource": "Condition",
      "primary_fields": ['code.text'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Data completeness",
      "metric_date_field": "recordedDate",
      "metric_date_description": "Condition recorded date",
      "dimension_a": "clinical_status",
      "dimension_a_description": "The condition status bound to http://terminology.hl7.org/CodeSystem/condition-clinical",
      "dimension_b": "verification_status",
      "dimension_b_description": "The condition verification status bound to http://terminology.hl7.org/CodeSystem/condition-ver-status",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      {{ code_from_codeableconcept('clinicalStatus', 'http://terminology.hl7.org/CodeSystem/condition-clinical') }} AS clinical_status,
      {{ code_from_codeableconcept('verificationStatus', 'http://terminology.hl7.org/CodeSystem/condition-ver-status') }} AS verification_status,
      {{ has_value('C.code.text') }} AS has_code_text
    FROM {{ ref('Condition') }} AS C
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(CAST(has_code_text AS '~type_long()~'))',
    denominator = 'COUNT(id)'
) }}