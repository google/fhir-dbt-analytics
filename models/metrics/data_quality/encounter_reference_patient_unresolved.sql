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
      "dimension_a_name": "Status",
      "dimension_a_description": "The status of the encounter (planned | arrived | triaged | in-progress | onleave | finished | cancelled +)",
    }
) -}}

-- depends_on: {{ ref('Encounter') }}
-- depends_on: {{ ref('Patient') }}
{%- if fhir_resource_exists('Encounter') %}

WITH
  A AS (
    SELECT
      id,
      fhir_mapping,
      metric_date,
      source_system,
      site,
      data_transfer_type,
      {{ get_column_or_default('status') }} AS status,
      {{ get_column_or_default('class.code') }} AS encounter_class,
      (
        SELECT SIGN(COUNT(*))
        FROM {{ ref('Patient') }} AS P
        {%- if column_exists('subject.patientId', 'Encounter') %}
        WHERE E.subject.patientId = P.id
        {%- elif column_exists('subject.reference', 'Encounter') %}
        WHERE E.subject.reference = P.id AND E.subject.type = 'Patient'
        {%- else %}
        WHERE FALSE
        {%- endif %}
      ) AS reference_patient_resolved    
    FROM {{ ref('Encounter') }} AS E
  )
SELECT
  CURRENT_DATETIME() as execution_datetime,
  '{{this.name}}' AS metric_name,
  fhir_mapping AS fhir_mapping,
  source_system AS source_system,
  data_transfer_type AS data_transfer_type,
  metric_date AS metric_date,
  site AS site,
  CAST(status AS STRING) AS slice_a,
  CAST(encounter_class AS STRING) AS slice_b,
  CAST(NULL AS STRING) AS slice_c,
  SUM(1 - reference_patient_resolved) AS numerator,
  COUNT(id) AS denominator_cohort,
  CAST(SAFE_DIVIDE(SUM(1 - reference_patient_resolved), COUNT(id)) AS FLOAT64) AS measure
FROM A
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

{%- else %}
{{- empty_metric_output() -}}
{%- endif -%}