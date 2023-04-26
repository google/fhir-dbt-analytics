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
      "description": "Proportion of Person resources that do not have at least one reference to a patient recorded",
      "short_description": "Person ref. Patient - unrecorded",
      "primary_resource": "Person",
      "primary_fields": ['link.target.patientId'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "dimension_a": "person_active",
      "dimension_a_description": "The person active status  (true | false)",
      "dimension_b": "person_gender",
      "dimension_b_description": "The gender of the person (male | female | other | unknown)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      CAST({{ get_column_or_default('active') }} AS STRING) person_active,
      CAST({{ get_column_or_default('gender') }} AS STRING) as person_gender,
      ( SELECT SIGN(COUNT(*)) FROM UNNEST(link)  WHERE target.patientId IS NOT NULL ) AS has_patient_id
    FROM {{ ref('Person') }}
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(1 - has_patient_id)',
    denominator = 'COUNT(DISTINCT id)'
) }}