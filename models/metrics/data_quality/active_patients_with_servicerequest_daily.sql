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
      "description": "Proportion of active patients with at least one service request that day",
      "short_description": "Patients with ServiceRequest",
      "primary_resource": "ServiceRequest",
      "primary_fields": ['subject.patientId', 'encounter.encounterId', 'authoredOn'],
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
      S.subject.patientId AS servicerequest_patientid
    FROM ({{ active_encounters() }}) AS ActiveEncounters
    LEFT JOIN {{ ref('ServiceRequest') }} AS S
      ON ActiveEncounters.id = S.encounter.encounterId
      AND ActiveEncounters.metric_date = S.metric_date
{%- endset -%}

{{- calculate_metric(
    metric_sql,
    numerator = 'COUNT(DISTINCT servicerequest_patientid)',
    denominator = 'COUNT(DISTINCT patientId)'
) -}}