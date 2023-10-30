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
    materialized = 'incremental'
) -}}

WITH LatestExecution AS (
  SELECT execution_id, execution_date, execution_datetime, metric_name, MIN(metric_date) AS min_metric_date, MAX(metric_date) AS max_metric_date, COUNT(*) AS row_count
  FROM {{ ref('metric_latest_execution') }}
  GROUP BY 1,2,3,4
)
SELECT
  execution_id,
  execution_date,
  ARRAY_AGG(STRUCT(metric_name, execution_datetime, min_metric_date, max_metric_date, row_count)) AS metric
FROM LatestExecution
GROUP BY 1, 2