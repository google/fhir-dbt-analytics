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
      "description": "Patient birthdate distribution",
      "short_description": "Patient dob distribution",
      "primary_resource": "Patient",
      "primary_fields": ['birthdate'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Data distribution",
      "dimension_a": "decade",
      "dimension_a_description": "The patient's birth decade'",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      CAST(ROUND(SAFE_CAST(SUBSTR(P.birthdate,0,4) AS {{ fhir_dbt_utils.type_long() }}),-1) AS STRING) AS decade,
    FROM {{ ref('Patient') }} AS P
{%- endset -%}

{{ calculate_metric(
    metric_sql
) }}