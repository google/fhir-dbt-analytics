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
    materialized='ephemeral',
    enabled = var('patient_panel_enabled')
) -}}

 WITH empi AS (
  SELECT *
  FROM {{ ref('empi_patient_crosswalk') }}
 ),
    chronic_condition AS (
      SELECT
        master_patient_id,
        ARRAY_AGG(
          (
            SELECT AS STRUCT
              group_name AS chronic_condition,
              CAST(first_documented AS DATETIME) AS first_documented,
              CAST(last_documented AS DATETIME) AS last_documented,
              number_documented AS number_instances_coded
          )) AS chronic_condition_array
      FROM
        (
          SELECT
            {{ condition_metadata('diabetes') }}
          FROM  {{ ref('diabetes') }} diabetes
          UNION ALL
          SELECT
            {{ condition_metadata('copd') }}
          FROM  {{ ref('copd') }} copd
          UNION ALL
          SELECT DISTINCT
            {{ condition_metadata('ckd') }}
          FROM {{ ref('ckd') }} ckd
          UNION ALL
          SELECT DISTINCT
            {{ condition_metadata('hf') }}
          FROM {{ ref('heart_failure') }} hf
          UNION ALL
          SELECT DISTINCT
            {{ condition_metadata('ht') }}
          FROM {{ ref('hypertension') }} ht
        )
      GROUP BY master_patient_id
    )
, condition_data AS (
      SELECT
        empi.master_patient_id,
        COUNT(DISTINCT cc.code) AS unique_icd_code_count,
        COUNT(DISTINCT IF(LOWER(cc.code) NOT LIKE 'z%' , cc.code, NULL)) AS unique_condition_code_count,
        ARRAY_AGG(DISTINCT cc.code) AS icd_list,
        ARRAY_AGG(
            (SELECT AS STRUCT
              cc.code AS condition_code,
              SUBSTR(c.recordedDate,0,10) as condition_date
          )) AS icd_struct_list
      FROM empi
      JOIN {{ ref('Condition') }} c
        ON c.subject.patientId = empi.patient_id
      JOIN UNNEST(c.code.coding) cc
      WHERE
        (
         c.verificationStatus IS NULL
        OR
          {{ code_from_codeableconcept(
          'verificationStatus',
          'http://terminology.hl7.org/CodeSystem/condition-ver-status',
          index=0
          ) }} NOT IN ('entered-in-error'))
        AND cc.system LIKE '%icd%'
      GROUP BY empi.master_patient_id
    ),
    procedure_data AS (
      SELECT
        empi.master_patient_id,
        COUNT(DISTINCT IF(cc.system LIKE '%icd%', cc.code, NULL)) AS unique_icd_code_count,
        COUNT(DISTINCT IF(cc.system LIKE '%cpt%', cc.code, NULL)) AS unique_cpt_code_count,
        ARRAY_AGG(DISTINCT IF(cc.system LIKE '%icd%', cc.code, NULL) IGNORE NULLS) AS icd_list,
        ARRAY_AGG(DISTINCT IF(cc.system LIKE '%cpt%', cc.code, NULL) IGNORE NULLS) AS cpt_list,
        ARRAY_AGG(
            (SELECT AS STRUCT
              cc.code AS procedure_code,
              SUBSTR(p.performed.dateTime,0,10) as procedure_date
          )) AS icd_struct_list,
          ARRAY_AGG(
            (SELECT AS STRUCT
              cc.code AS procedure_code,
              SUBSTR(p.performed.dateTime,0,10) as procedure_date
          )) AS cpt_struct_list
      FROM empi
      JOIN {{ ref('Procedure') }} p
        ON p.subject.patientId = empi.patient_id
      JOIN UNNEST(p.code.coding) cc
      WHERE
        p.status NOT IN ('entered-in-error')
        AND (cc.system LIKE '%icd%' OR cc.system LIKE '%cpt%')
      GROUP BY empi.master_patient_id
    )

  SELECT
    empi.master_patient_id,
    condition.chronic_condition_array,
    cd.unique_icd_code_count AS unique_icd_dx_code_count,
    cd.unique_condition_code_count,
    cd.icd_list AS icd_dx_list,
    cd.icd_struct_list AS icd_dx_struct_list,
    pd.unique_icd_code_count AS unique_icd_pc_code_count,
    pd.unique_cpt_code_count AS unique_cpt_pc_code_count,
    pd.cpt_struct_list AS cpt_pc_struct_list,
    pd.icd_struct_list AS icd_pc_struct_list,
    pd.icd_list AS icd_pc_list,
    pd.cpt_list AS cpt_pc_list

  FROM empi
  LEFT JOIN chronic_condition condition
    ON empi.master_patient_id = condition.master_patient_id
  LEFT JOIN condition_data cd
    ON empi.master_patient_id = cd.master_patient_id
  LEFT JOIN procedure_data pd
    ON empi.master_patient_id = pd.master_patient_id


