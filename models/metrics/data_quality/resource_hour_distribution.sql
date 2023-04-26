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
        {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'Encounter' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('Encounter')}} AS E
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'AllergyIntolerance' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('AllergyIntolerance')}}
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'Composition' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('Composition')}}
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'Condition' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('Condition')}}
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'DiagnosticReport' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('DiagnosticReport')}}
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'DocumentReference' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('DocumentReference')}}
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'MedicationRequest' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('MedicationRequest')}}
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'MedicationAdministration' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('MedicationAdministration')}}
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'MedicationStatement' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('MedicationStatement')}}
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'Observation' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('Observation')}}
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'Procedure' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('Procedure')}}
      GROUP BY 1, 2, 3, 4, 5, 6
      UNION ALL
      SELECT
      {{-metric_common_dimensions(exclude_col = 'metric_date')}}
        'ServiceRequest' AS resource,
        EXTRACT(HOUR FROM metric_hour) AS hour_of_day,
        COUNT(*) AS resource_count
      FROM {{ref('ServiceRequest')}}
      GROUP BY 1, 2, 3, 4, 5, 6
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