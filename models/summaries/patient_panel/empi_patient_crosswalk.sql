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
    materialized = 'table',
    enabled = var('patient_panel_enabled')
) -}}

 {%- if var('empi_as_person_enabled') == TRUE -%}

  SELECT
      P.id AS master_patient_id
      l.target.patientId AS patient_id
  FROM {{ ref('Person') }} AS P, UNNEST(P.link) AS l
  JOIN {{ ref('Patient') }} AS pat
    ON l.target.patientId = pat.id  

  {%- else -%}

  SELECT
      P.id AS master_patient_id,
      P.id AS patient_id
  FROM {{ ref('Patient') }} AS P

  {%- endif -%} 


