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
      "description": "Count of valid Patient resources",
      "short_description": "Patient resources",
      "primary_resource": "Patient",
      "primary_fields": ['id'],
      "secondary_resources": ['Encounter'],
      "calculation": "COUNT",
      "category": "Resource count",
      "metric_date_field": "Encounter.period.start",
      "metric_date_description": "Encounter start date of patient's earliest clinical encounter",
      "dimension_a": "active",
      "dimension_a_description": "Whether this patient's record is in active use",
      "dimension_b": "gender",
      "dimension_b_description": "The patient's gender (male, female, other, unknown)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions(exclude_col='metric_date') }}
      (
        SELECT MIN(metric_date)
        FROM {{ ref('Encounter') }} AS E
        WHERE P.id = E.subject.patientId
        AND E.class.code NOT IN (
          'OTHER',
          'PRENC',
          'LAB',
          'UNKNOWN',
          'HIST'
        )
      ) AS metric_date,
      CAST({{ get_column_or_default('active') }} AS STRING) AS active,
      gender
    FROM {{ ref('Patient') }} AS P
{%- endset -%}  

{{- calculate_metric(metric_sql) -}}