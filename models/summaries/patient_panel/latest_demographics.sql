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

{{config(materialized = 'table', enabled = var('patient_panel_enabled'))
-}}
WITH empi AS ( 
  SELECT *
  FROM {{ ref('empi_patient_crosswalk') }}
),
  latest_encounter AS (
    SELECT
      empi.master_patient_id,
      empi.patient_id,
      e.start_date,
      ROW_NUMBER()
      OVER (PARTITION BY empi.master_patient_id ORDER BY e.start_date DESC) AS enc_num_desc
      FROM empi
      LEFT JOIN {{ ref('encounter_summary') }} e
        ON empi.patient_id = e.patient_id
  ),
  ,patient_data AS (
      SELECT
        p.id AS patient_id,
        empi.master_patient_id,
      {%- if column_exists('p.identifier.type','Patient') %}
        ARRAY(
          SELECT DISTINCT value 
          FROM UNNEST(p.identifier) i, UNNEST(i.type.coding) t 
          WHERE t.system = 'http://terminology.hl7.org/CodeSystem/v2-0203'
          ) AS mrns,
      {%- else %}
       ARRAY(SELECT NULL) AS mrns,
      {%- endif %}
      {%- if column_exists('race', 'Patient') %}
        ARRAY_TO_STRING(
          ARRAY(
            SELECT DISTINCT display 
            FROM UNNEST(p.race.ombCategory) 
            ORDER BY display
            ),
          ',') AS race,
        {%- else %}
        CAST(NULL AS STRING) AS race,
        {%- endif %}
        {%- if column_exists('p.ethnicity.ombCategory.display','Patient') %}
        p.ethnicity.ombCategory.display AS ethnicity,
        {%- else %}
        CAST(NULL AS STRING) AS ethnicity,
        {%- endif %}
        {{ site() }} AS site,
        SAFE_CAST(p.deceased.dateTime AS DATE) AS deceased_date,
        SAFE_CAST(p.birthDate AS DATE) AS birth_date,
        p.gender,
        {%- if column_exists('p.address', 'Patient') %}
        SUBSTR(
          COALESCE(
            (SELECT postalCode
            FROM UNNEST(p.address)
            WHERE
              use = 'home' AND (period.end IS NULL OR period.end >= CAST(current_date() AS STRING))
            LIMIT 1),
            p.address[SAFE_OFFSET(0)].postalCode
          ),
          0,
          5) AS zipcode
        {%- else %}
        CAST(NULL AS STRING) AS zipcode
        {%- endif %}
      FROM empi
      JOIN {{ ref('Patient_view') }} p
        ON empi.patient_id = p.id
      {%- if column_exists('active','Patient') %}
      WHERE
       p.active IS NULL OR p.active = TRUE
     {%- endif %}
      
    ),
    person_aggregates
    AS (
      SELECT
        pat.master_patient_id,
        ARRAY_AGG(DISTINCT pat.patient_id IGNORE NULLS) AS patient_ids,
        ARRAY_AGG(DISTINCT pmrn IGNORE NULLS) AS patient_mrns,
        ARRAY_AGG(DISTINCT pat.site IGNORE NULLS) AS site,
        ARRAY_AGG(DISTINCT pat.deceased_date IGNORE NULLS) AS deceased_date,
        ARRAY_AGG(DISTINCT pat.birth_date IGNORE NULLS) AS birth_date,
        ARRAY_AGG(DISTINCT pat.gender IGNORE NULLS) AS gender,
        ARRAY_AGG(DISTINCT pat.zipcode IGNORE NULLS) AS zipcode,
        ARRAY_AGG(DISTINCT pat.race IGNORE NULLS) AS race,
        ARRAY_AGG(DISTINCT pat.ethnicity IGNORE NULLS) AS ethnicity,
      FROM patient_data AS pat
      LEFT JOIN UNNEST(pat.mrns) pmrn
      GROUP BY pat.master_patient_id
    ),
    latest_flag AS (
      SELECT
        pat.patient_id,
        pat.master_patient_id,
        pat.site,
        pat.deceased_date,
        pat.birth_date,
        pat.gender,
        pat.zipcode,
        pat.race,
        pat.ethnicity,
        enc.start_date,
        IF(enc.enc_num_desc IS NOT NULL, TRUE, FALSE) as latest_dem_flag
      FROM patient_data pat
      LEFT JOIN latest_encounter enc
        ON pat.master_patient_id = enc.master_patient_id
        AND enc.enc_num_desc = 1
    )
    SELECT
      pa.master_patient_id,
      COALESCE(lf.site, pa.site[SAFE_OFFSET(0)]) AS site,
      COALESCE(lf.deceased_date, pa.deceased_date[SAFE_OFFSET(0)]) AS deceased_date,
      COALESCE(lf.birth_date, pa.birth_date[SAFE_OFFSET(0)]) AS birth_date,
      COALESCE(lf.gender, pa.gender[SAFE_OFFSET(0)]) AS gender,
      COALESCE(lf.zipcode, pa.zipcode[SAFE_OFFSET(0)]) AS zipcode,
      COALESCE(lf.race, pa.race[SAFE_OFFSET(0)]) AS race,
      COALESCE(lf.ethnicity, pa.ethnicity[SAFE_OFFSET(0)]) AS ethnicity,
      pa.patient_ids,
      lf.start_date AS latest_encounter_start_date,
      pa.patient_mrns,
      lf.payor
    FROM person_aggregates pa
    LEFT JOIN latest_flag lf
        ON lf.master_patient_id = pa.master_patient_id
        AND latest_dem_flag = TRUE
