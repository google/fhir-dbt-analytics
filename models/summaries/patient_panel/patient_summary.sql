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
    materialized='table',
    enabled = var('patient_panel_enabled')
) -}}

WITH pp AS(
  SELECT
    ld.master_patient_id,
    ld.patient_ids,
    ld.patient_mrns,
    ld.site,
    ld.deceased_date,
    ld.birth_date,
    {%- if var('age_calculation_method') == "age_based_on_last_encounter" %}
    'age_based_on_last_encounter' AS age_method,
    DATE_DIFF(DATE(enc.last_encounter_start),(ld.birth_date), YEAR) AS age_in_years,
    {%- else %}
    'age_as_of_cohort_snapshot' AS age_method,
    {{ age(date_of_birth='birth_date') }} AS age_in_years,
    {%- endif %}
    ld.gender,
    ld.zipcode,
    CASE WHEN LOWER(ld.race) = 'asked but no answer' OR LOWER(race) = 'unknown' OR race = '' THEN 'Unknown'
          WHEN race = 'White' THEN 'White'
          ELSE 'Non-White'
        END AS race_group,
    ld.race,
    ld.ethnicity,
    CASE 
      WHEN ld.race LIKE '%,%' THEN 'Two or more races'
      WHEN LOWER(ld.ethnicity) = 'hispanic or latino' THEN 'Hispanic or Latino'
      WHEN LOWER(ld.ethnicity) = 'not hispanic or latino' AND LOWER(ld.race)='white' THEN 'Not Hispanic White'
      WHEN LOWER(ld.ethnicity) = 'not hispanic or latino' AND LOWER(ld.race)='black or african american' THEN 'Not Hispanic Black or African American'
      WHEN LOWER(ld.race)='native hawaiian or other pacific islander' THEN 'Native Hawaiian or Other Pacific Islander'
      WHEN LOWER(ld.race)='american indian or alaska native' THEN 'American Indian or Alaska Native'
      WHEN LOWER(ld.race)='asian' THEN 'Asian'
      WHEN LOWER(ld.race) LIKE 'unknown' THEN NULL
      ELSE 'Some Other Race' 
    END AS race_ethnicity_composite,
    ld.payor,
    enc.services, 
    IF(
      last_encounter_start > DATE_SUB({{ get_snapshot_date() }}, INTERVAL 5 YEAR),
      1,
      0) AS enc_last_5_years_flag,
    IF(
      last_encounter_start > DATE_SUB({{ get_snapshot_date() }}, INTERVAL 1 YEAR),
      1,
      0) AS enc_last_1_year_flag,
    IF(
      last_encounter_start > DATE_SUB({{ get_snapshot_date() }}, INTERVAL 6 MONTH),
      1,
      0) AS enc_last_6_month_flag,
    IF(
      last_encounter_start > DATE_SUB({{ get_snapshot_date() }}, INTERVAL 3 MONTH),
      1,
      0) AS enc_last_3_month_flag,
    IF(
      last_amb_enc > DATE_SUB({{ get_snapshot_date() }}, INTERVAL 5 YEAR),
      1,
      0) AS amb_enc_last_5_years_flag,
    IF(
      last_amb_enc > DATE_SUB({{ get_snapshot_date() }}, INTERVAL 1 YEAR),
      1,
      0) AS amb_enc_last_1_year_flag,
    IF(
      last_amb_enc > DATE_SUB({{ get_snapshot_date() }}, INTERVAL 6 MONTH),
      1,
      0) AS amb_enc_last_6_month_flag,
    IF(
      last_amb_enc > DATE_SUB({{ get_snapshot_date() }}, INTERVAL 3 MONTH),
      1,
      0) AS amb_enc_last_3_month_flag,
    enc.encounter_lookback_years,
    enc.first_amb_enc,
    enc.last_amb_enc,
    enc.num_amb_enc_last_lookback_yrs,
    enc.first_imp_enc,
    enc.last_imp_enc,
    enc.last_imp_5_los,
    enc.num_imp_enc_last_lookback_yrs,
    enc.first_emer_discharge,
    enc.last_emer_discharge,
    enc.num_emer_discharge_last_lookback_yrs,
    enc.first_emer_admission,
    enc.last_emer_admission,
    enc.num_emer_admission_last_lookback_yrs,
    enc.first_encounter_start,
    enc.last_encounter_start,
    cp.chronic_condition_array,
    cp.unique_icd_dx_code_count,
    cp.unique_condition_code_count,
    cp.icd_dx_list,
    cp.icd_dx_struct_list,
    cp.unique_icd_pc_code_count,
    cp.unique_cpt_pc_code_count,
    cp.icd_pc_list,
    cp.cpt_pc_list,
    cp.icd_pc_struct_list,
    cp.cpt_pc_struct_list,
    DATETIME_TRUNC(CURRENT_DATETIME(), MINUTE) AS last_run
  FROM {{ ref('latest_demographics') }} ld
  LEFT JOIN {{ ref('encounter_aggregate') }} enc
    ON enc.master_patient_id = ld.master_patient_id
  LEFT JOIN {{ ref('condition_procedure_summary') }} cp
    ON enc.master_patient_id = cp.master_patient_id
)
  SELECT * 
  FROM pp
  {%- if  var('minimum_age') != None %}
  WHERE
   age_in_years >= {{ var('minimum_age') }}
  {%- endif %}