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
      "description": "Count of valid MedicationRequest resources",
      "short_description": "MedicationRequest resources",
      "primary_resource": "MedicationRequest",
      "primary_fields": ['id'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Resource count",
      "metric_date_field": "MedicationRequest.authoredOn",
      "metric_date_description": "Allergy or intolerance recorded date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the medication request (active | on-hold | cancelled | completed | entered-in-error | stopped | draft | unknown)",
      "dimension_b": "intent",
      "dimension_b_description": "The intent (proposal | plan | order | original-order | reflex-order | filler-order | instance-order | option)",
      "dimension_c": "category",
      "dimension_c_description": "The category of the medication request (inpatient | outpatient | community | discharge)",
    }
) -}}

-- depends_on: {{ ref('MedicationRequest') }}
{%- if fhir_resource_exists('MedicationRequest') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      intent,
      {{ try_code_from_codeableconcept(
        'category',
        'http://terminology.hl7.org/CodeSystem/medicationrequest-category',
        index = get_source_specific_category_index()) }} AS category,
      FROM {{ ref('MedicationRequest') }}
  )
{{ calculate_metric() }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}