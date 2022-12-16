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
      "description": "Count of valid AllergyIntolerance resources",
      "short_description": "AllergyIntolerance resources",
      "primary_resource": "AllergyIntolerance",
      "primary_fields": ['id'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Resource count",
      "metric_date_field": "AllergyIntolerance.recordedDate",
      "metric_date_description": "Allergy or intolerance recorded date",
      "dimension_a": "clinical_status",
      "dimension_a_description": "The clinical status of the allergy/intollerance (active | inactive | resolved)",
      "dimension_b": "verification_status",
      "dimension_b_description": "The verification status of the allergy/intollerance (unconfirmed | confirmed | refuted | entered-in-error)",
    }
) -}}

-- depends_on: {{ ref('AllergyIntolerance') }}
{%- if fhir_resource_exists('AllergyIntolerance') %}

WITH
  A AS (
    SELECT
      id,
      {{- metric_common_dimensions() }}
      {{ try_code_from_codeableconcept('clinicalStatus', 'http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical') }} AS clinical_status,
      {{ try_code_from_codeableconcept('verificationStatus', 'http://terminology.hl7.org/CodeSystem/allergyintolerance-verification' ) }} AS verification_status
    FROM {{ ref('AllergyIntolerance') }}
  )
{{ calculate_metric() }}

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}