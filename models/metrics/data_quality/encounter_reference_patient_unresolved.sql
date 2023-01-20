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
      "description": "Proportion of Encounter resources that do not reference an existing patient",
      "short_description": "Enc ref. Patient - non-exist",
      "primary_resource": "Encounter",
      "primary_fields": ['subject.patientId'],
      "secondary_resources": ['Patient'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "Encounter.period.start",
      "metric_date_description": "Encounter start date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the encounter (planned | arrived | triaged | in-progress | onleave | finished | cancelled +)",
      "dimension_b": "latest_encounter_class",
      "dimension_b_description": "The latest class of the encounter",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      class.code AS latest_encounter_class,
      {%- if column_exists('subject.patientId') %}
      CAST(subject.patientId IN (SELECT id FROM {{ ref('Patient') }}) AS INT64) AS reference_patient_resolved
      {%- elif column_exists('subject.reference') %}
      CAST(subject.type = 'Patient' AND subject.reference IN (SELECT id FROM {{ ref('Patient') }}) AS INT64) AS reference_patient_resolved,
      {%- else %}
      0 AS reference_patient_resolved,
      {%- endif %}
    FROM {{ ref('Encounter') }} AS E
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(1 - reference_patient_resolved)',
    denominator = 'COUNT(id)'
) }}