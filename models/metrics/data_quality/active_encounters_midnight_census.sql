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
      "description": "Count of patients that have an active inpatient encounter at midnight each day",
      "short_description": "Census",
      "primary_resource": "Encounter",
      "primary_fields": ['classHistory.period.start', 'classHistory.period.end'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Encounter daily",
      "dimension_a": "episode_class",
      "dimension_a_description": "The class of the encounter episode",
      "dimension_b": "encounter_service_provider",
      "dimension_b_description": "The organization responsible for providing the services for this encounter",
    }
) -}}

{%- set metric_sql -%}
    WITH
      Episode AS (
        SELECT
          E.id,
          {{- metric_common_dimensions(exclude_col='metric_date')|indent(8) }}
          {{ metric_date(['C.period.start', 'E.period.start']) }} AS period_start,
          {{ metric_date(['C.period.end', 'E.period.end']) }} AS period_end,
          E.status AS encounter_status,
          CASE
            WHEN UPPER(COALESCE(C.class.code, E.class.code)) IN ('IMP', 'ACUTE', 'NONAC') THEN 'IMP/ACUTE/NONAC'
            ELSE COALESCE(C.class.code, E.class.code)
            END AS episode_class,
          serviceProvider.organizationId AS encounter_service_provider
        FROM {{ ref('Encounter') }} AS E
        LEFT JOIN UNNEST(E.classHistory) AS C
        WHERE
          UPPER(COALESCE(C.class.code, E.class.code)) IN ('IMP', 'ACUTE', 'NONAC', 'SS', 'OBSENC', 'EMER')
          AND E.status IN ('in-progress', 'finished')
      ),
      DS AS (
        {{ date_spine()}}
      )

    SELECT
      Episode.id,
      {{- metric_common_dimensions(exclude_col='metric_date')|indent }}
      DS.date_day AS metric_date,
      Episode.episode_class,
      Episode.encounter_service_provider,
    FROM Episode JOIN DS
      ON Episode.period_start < DS.date_day
      AND {{ cap_encounter_end_date(encounter_class='episode_class')|indent }} >= DS.date_day
{%- endset -%}

{{- calculate_metric(metric_sql) -}}