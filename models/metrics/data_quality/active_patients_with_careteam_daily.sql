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
      "description": "Proportion of active patients with at least one assigned care team that day",
      "short_description": "Patients with CareTeam",
      "primary_resource": "CareTeam",
      "primary_fields": ['subject.patientId', 'encounter.encounterId', 'period.start', 'period.end'],
      "secondary_resources": ['Encounter'],
      "calculation": "PROPORTION",
      "category": "Active patients with resource",
      "dimension_a": "encounter_class_group",
      "dimension_a_description": "The latest class of the encounter grouped into ambulatory or non-ambulatory",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      ActiveEncounters.*,
      C.subject.patientId AS careteam_patientid
    FROM ({{ active_encounters() }}) AS ActiveEncounters
    LEFT JOIN {{ ref('CareTeam') }} AS C
      ON ActiveEncounters.id = C.encounter.encounterId
      AND ActiveEncounters.metric_date >= {{ fhir_dbt_utils.metric_date(['C.period.start']) }}
      AND (
        ActiveEncounters.metric_date <= {{ fhir_dbt_utils.metric_date(['C.period.end']) }}
        OR C.period.end IS NULL
        OR C.period.end = ''
      )
{%- endset -%}

{{- calculate_metric(
    metric_sql,
    numerator = 'COUNT(DISTINCT careteam_patientid)',
    denominator = 'COUNT(DISTINCT patientId)'
) -}}