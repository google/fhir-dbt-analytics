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
        "description": "Distribution of resource counts by time of day",
        "short_description": "Resource hour distribution",
        "primary_resource": "Encounter",
        "primary_fields": ['period.start'],
        "secondary_resources": [],
        "calculation": "COUNT",
        "category": "Data distribution",
        "dimension_a": "hour_of_day",
        "dimension_a_description": "The hour of the day of the clinical timestamp of a resource",
        "dimension_b": "resource",
        "dimension_b_description": "The relevant resource used to extract the timestamp",
      }
 ) -}}
{%- set metric_sql -%}

    WITH combined_resources AS (
      SELECT
        {{-metric_common_dimensions(include_metric_date=False, fhir_resource='Encounter')}}
        'Encounter' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('Encounter')}} AS E
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='AllergyIntolerance')}}
        'AllergyIntolerance' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('AllergyIntolerance')}}
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='Composition')}}
        'Composition' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('Composition')}}
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='Condition')}}
        'Condition' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('Condition')}}
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='DiagnosticReport')}}
        'DiagnosticReport' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('DiagnosticReport')}}
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='DocumentReference')}}
        'DocumentReference' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('DocumentReference')}}
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='MedicationRequest')}}
        'MedicationRequest' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('MedicationRequest')}}
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='MedicationAdministration')}}
        'MedicationAdministration' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('MedicationAdministration')}}
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='MedicationStatement')}}
        'MedicationStatement' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('MedicationStatement')}}
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='Observation')}}
        'Observation' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('Observation')}}
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='Procedure')}}
        'Procedure' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('Procedure')}}
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
      {{-metric_common_dimensions(include_metric_date=False, fhir_resource='ServiceRequest')}}
        'ServiceRequest' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('ServiceRequest')}}
      GROUP BY 1, 2, 3, 4, 5
)
SELECT
  *,
  CAST(NULL AS DATE) AS metric_date
  FROM combined_resources
{%- endset -%}
{{ calculate_metric(
    metric_sql,
    measure= 'SUM(resource_count)'
)
}}