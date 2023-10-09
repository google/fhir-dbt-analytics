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

{{config(materialized = 'ephemeral', enabled = var('patient_panel_enabled'))
-}}

SELECT
  empi.master_patient_id,
  ARRAY_AGG(DISTINCT e.service IGNORE NULLS) AS services,
  MIN(IF(e.class = 'AMB', e.start_date, NULL)) AS first_amb_enc,
  MAX(IF(e.class = 'AMB', e.start_date, NULL)) AS last_amb_enc,
  var('encounter_lookback') AS encounter_lookback_years,
  COUNT(
    DISTINCT
      IF(
        e.class = 'AMB'
          AND e.start_date > DATE_SUB({{get_snapshot_date()}}, INTERVAL var('encounter_lookback') YEAR),
        e.encounter_id,
        NULL)) AS num_amb_enc_last_lookback_yrs,
  MIN(IF(e.class = 'IMP', e.start_date, NULL)) AS first_imp_enc,
  MAX(IF(e.class = 'IMP', e.start_date, NULL)) AS last_imp_enc,
  MAX(IF(e.class = 'IMP' AND LOS >= 5, e.start_date, NULL)) AS last_imp_5_los,
  COUNT(
    DISTINCT
      IF(
        e.class = 'IMP'
          AND e.start_date > DATE_SUB({{get_snapshot_date()}}, INTERVAL var('encounter_lookback') YEAR),
        e.encounter_id,
        NULL)) AS num_imp_enc_last_lookback_yrs,
  MIN(IF(e.class = 'EMER', e.start_date, NULL)) AS first_emer_discharge,
  MAX(IF(e.class = 'EMER', e.start_date, NULL)) AS last_emer_discharge,
  COUNT(
    DISTINCT
      IF(
        e.class = 'EMER'
          AND e.start_date > DATE_SUB({{get_snapshot_date()}}, INTERVAL var('encounter_lookback') YEAR),
        e.encounter_id,
        NULL)) AS num_emer_discharge_last_lookback_yrs,
  MIN(IF(emergency_adm_flag = TRUE, e.start_date, NULL))
    AS first_emer_admission,
  MAX(IF(emergency_adm_flag = TRUE, e.start_date, NULL))
    AS last_emer_admission,
  COUNT(
    DISTINCT
      IF(
        (emergency_adm_flag = TRUE)
          AND e.start_date > DATE_SUB({{get_snapshot_date()}}, INTERVAL var('encounter_lookback') YEAR),
        e.encounter_id,
        NULL))
    AS num_emer_admission_last_lookback_yrs,
  MIN(e.start_date) AS first_encounter_start,
  MAX(e.start_date) AS last_encounter_start
FROM {{ ref('empi_patient_crosswalk') }} empi
LEFT JOIN {{ref('encounter_summary')}} e
  ON e.patient_id = empi.patient_id
GROUP BY 1
