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

{{- config(
    materialized = 'ephemeral',
    meta = {
      "cohort_description": "Adults with a diagnosis of hypertension and BMI>25 without a recorded medication reconciliation in the past 12 months"
      }
) -}}

SELECT
  '{{this.name}}' AS cohort_name,
  {{ get_snapshot_date() }} AS cohort_snapshot_date,
  P.id AS patient_id
FROM {{ ref('Patient_view') }} AS P
WHERE {{ alive() }}
AND {{ age() }} >= 18
AND {{ has_encounter(class=['AMB', 'EMER', 'IMP'], lookback='5 YEAR') }}
AND {{ has_condition('Hypertension') }}
AND {{ has_medication_request('Anti-hypertensive medication', lookback='12 MONTH') }}
AND {{ has_observation('BMI', value_greater_than=25, lookback='12 MONTH') }}
AND NOT {{ has_procedure('Medication Reconciliation', lookback='12 MONTH') }}