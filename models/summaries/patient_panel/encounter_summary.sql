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

-- depends_on: {{ ref('site_timezones') }}

{{ config(
    materialized='table',
    enabled = var('patient_panel_enabled'),
    meta = {
      "fhir_resource": "Encounter"
    }
) -}}

WITH encounter AS (
  {{ has_encounter(class=['AMB', 'IMP','SS', 'OBSENC','NONAC','EMER'],status=['in-progress', 'finished'], lookback= var('encounter_lookback_years'), return_all=True) }}
)
  SELECT
    e.encounter.id AS encounter_id,
    e.encounter.patientid AS patient_id,
    e.encounter.discharge_disposition,
    e.encounter.start_date,
    e.encounter.end_date,
    e.encounter.status,
    e.encounter.class,
    e.encounter.service,
    e.encounter.emergency_adm_flag,
    e.encounter.LOS
    {%- if var('disease_specific') == 'AKI' -%}
    ,'AKI' AS disease_specific_cohort,
    IF(MA.first_documented IS NULL,0,1) AS medication_admin_flag,
    IF(MR.first_documented IS NULL,0,1) AS medication_request_flag,
    IF(O.first_documented IS NULL,0,1) AS observation_flag,
    IF(P.first_documented IS NULL,0,1) AS procedure_flag,
    ARRAY_AGG((
      SELECT AS STRUCT
      MA.group_name AS medication_admin_group_name,
      MA.first_documented,
      MA.last_documented,
      MA.number_documented)) AS medication_admin_documentation,
    ARRAY_AGG((
      SELECT AS STRUCT
      MR.group_name AS medication_request_group_name,
      MR.first_documented,
      MR.last_documented,
      MR.number_documented)) AS medication_request_documentation,
    ARRAY_AGG((
      SELECT AS STRUCT
      O.group_name AS observation_group_name,
      O.first_documented,
      O.last_documented,
      O.unit,
      O.median_documented,
      O.pct90_documented,
      O.pct95_documented,
      O.pct99_documented,
      O.avg_documented,
      O.stddev_documented,
      O.number_documented)) AS observation_documentation,
    ARRAY_AGG((
      SELECT AS STRUCT
      P.group_name AS procedure_group_name,
      P.first_documented,
      P.last_documented,
      P.number_documented)) AS procedure_documentation

  FROM  encounter e
  LEFT JOIN {{ ref('medication_admin_coverage') }} MA
    ON MA.encounter_id=e.encounter.id
  LEFT JOIN {{ ref('medication_request_coverage') }} MR
    ON MR.encounter_id=e.encounter.id
  LEFT JOIN {{ ref('observation_coverage') }} O
    ON O.encounter_id=e.encounter.id
  LEFT JOIN {{ ref('procedure_coverage') }} P
    ON P.encounter_id=e.encounter.id
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
{%- else -%}
  FROM  encounter e
{%- endif %}
