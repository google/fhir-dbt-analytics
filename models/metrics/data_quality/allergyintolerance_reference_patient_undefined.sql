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
      "description": "Proportion of AllergyIntolerance resources that do not have a patient reference recorded",
      "short_description": "Allergy ref. Patient - unrecorded",
      "primary_resource": "AllergyIntolerance",
      "primary_fields": ['patient.patientId'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "AllergyIntolerance.recordedDate",
      "metric_date_description": "Allergy or intolerance recorded date",
      "dimension_a": "clinical_status",
      "dimension_a_description": "The clinical status of the allergy/intollerance (active | inactive | resolved)",
      "dimension_b": "verification_status",
      "dimension_b_description": "The verification status of the allergy/intollerance (unconfirmed | confirmed | refuted | entered-in-error)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      {{ fhir_dbt_utils.code_from_codeableconcept('clinicalStatus', 'http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical') }} AS clinical_status,
      {{ fhir_dbt_utils.code_from_codeableconcept('verificationStatus', 'http://terminology.hl7.org/CodeSystem/allergyintolerance-verification') }} AS verification_status,
      {{ has_reference_value('patient', 'Patient') }} AS has_reference_value
    FROM {{ ref('AllergyIntolerance') }} AS A
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(1 - has_reference_value)',
    denominator = 'COUNT(id)'
) }}
