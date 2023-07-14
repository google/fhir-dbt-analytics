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
      "description": "Count of encounters ending during each day",
      "short_description": "Discharged encounters",
      "primary_resource": "Encounter",
      "primary_fields": ['period.end'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Encounter daily",
      "metric_date_field": "Encounter.period.end",
      "metric_date_description": "Encounter end date",
      "dimension_a": "latest_encounter_class",
      "dimension_a_description": "The latest class of the encounter",
      "dimension_b": "encounter_service_provider",
      "dimension_b_description": "The organization responsible for providing the services for this encounter",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions(exclude_col='metric_date') }}
      {{ metric_date(['period.end']) }} AS metric_date,
      {{ get_column_or_default('serviceProvider.organizationId', 'Encounter') }} AS encounter_service_provider,
      CASE
        WHEN UPPER(class.code) IN ('IMP', 'ACUTE', 'NONAC') THEN 'IMP/ACUTE/NONAC'
        WHEN UPPER(class.code) IN ('IMPPS', 'IMPRE', 'OTHER') THEN 'IMPPS/IMPRE/OTHER'
        ELSE class.code END AS latest_encounter_class
    FROM {{ ref('Encounter') }}
    WHERE
      UPPER(class.code) IN ('IMP', 'ACUTE', 'NONAC', 'SS', 'OBSENC', 'EMER', 'IMPPS', 'IMPRE', 'OTHER')
      AND status NOT IN ('cancelled', 'entered-in-error')
      AND period.end IS NOT NULL
      AND period.end <> ''
{%- endset -%}

{{- calculate_metric(metric_sql) -}}