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
      "description": "Proportion of Composition resources that reference a non-existent binary resource",
      "short_description": "Comp ref. Binary - non-exist",
      "primary_resource": "Composition",
      "primary_fields": ['section.entry.binaryId'],
      "secondary_resources": ['Binary'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "Composition.date",
      "metric_date_description": "Composition latest edit date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the composition (preliminary | final | amended | entered-in-error)",
    }
) -}}

-- depends_on: {{ ref('Binary') }}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      (
        SELECT SIGN(COUNT(*))
        FROM {{ fhir_dbt_utils.spark_parenthesis(fhir_dbt_utils.unnest_multiple([
          fhir_dbt_utils.array_config("C.section", "CS"),
          fhir_dbt_utils.array_config("CS.entry", "CSE")])) }}
        WHERE CSE.binaryid IS NOT NULL
        AND CSE.binaryid <> ''
      ) AS has_reference_value,
      (
        SELECT SIGN(COUNT(*))
        FROM {{ fhir_dbt_utils.spark_parenthesis(fhir_dbt_utils.unnest_multiple([
          fhir_dbt_utils.array_config("C.section", "CS"),
          fhir_dbt_utils.array_config("CS.entry", "CSE")])) }}
        JOIN {{ ref('Binary') }} AS B
          ON CSE.binaryid = B.id
      ) AS reference_resolves
    FROM {{ ref('Composition') }} AS C
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(has_reference_value - reference_resolves)',
    denominator = 'COUNT(id)'
) }}
