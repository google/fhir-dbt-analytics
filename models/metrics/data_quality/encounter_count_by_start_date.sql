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
    tags = ["spark_todo"],
    meta = {
      "description": "Count of encounters starting each day",
      "short_description": "Admitted encounters",
      "primary_resource": "Encounter",
      "primary_fields": ['period.start', 'classHistory'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Encounter daily",
      "metric_date_field": "Encounter.period.start",
      "metric_date_description": "Encounter start date",
      "dimension_a": "initial_encounter_class",
      "dimension_a_description": "The initial class recorded for the encounter, ignoring pre-admission class (PRENC)",
      "dimension_b": "encounter_service_provider",
      "dimension_b_description": "The organization responsible for providing the services for this encounter",
    }
) -}}

{%- set metric_sql -%}
    WITH
      A AS (
        SELECT
          *,
          COALESCE(
            (
              SELECT UPPER(H.class.code)
              FROM UNNEST(classHistory) H
              WHERE H.class.code IN ('IMP', 'ACUTE', 'NONAC', 'SS', 'OBSENC', 'EMER', 'AMB', 'IMPPS', 'IMPRE', 'OTHER')
              ORDER BY H.period.start
              LIMIT 1
            ),
            UPPER(E.class.code)
          ) AS initial_encounter_class
        FROM {{ ref('Encounter') }} AS E
        WHERE
          UPPER(E.class.code) IN ('IMP', 'ACUTE', 'NONAC', 'SS', 'OBSENC', 'EMER', 'AMB', 'IMPPS', 'IMPRE', 'OTHER')
          AND E.status NOT IN ('cancelled', 'entered-in-error')
          AND E.period.start IS NOT NULL
          AND E.period.start <> ''
      )
      SELECT
        id,
        {{- metric_common_dimensions() }}
        {{ get_column_or_default('serviceProvider.organizationId', 'Encounter') }} AS encounter_service_provider,
        CASE
          WHEN initial_encounter_class IN ('IMP', 'ACUTE', 'NONAC') THEN 'IMP/ACUTE/NONAC'
          WHEN initial_encounter_class IN ('IMPPS', 'IMPRE','OTHER') THEN 'IMPPS/IMPRE/OTHER'
          ELSE initial_encounter_class END AS initial_encounter_class
      FROM A
{%- endset -%}

{{- calculate_metric(metric_sql) -}}