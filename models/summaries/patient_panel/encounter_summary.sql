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
    materialized='view',
    enabled = var('patient_panel_enabled')
) -}}

WITH encounter AS (
  {{ has_encounter(class=['AMB', 'IMP','SS', 'OBSENC','NONAC','EMER'],status=['in-progress', 'finished'], lookback= var('encounter_lookback'), return_all=TRUE) }}
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
  FROM  encounter e
  
      
