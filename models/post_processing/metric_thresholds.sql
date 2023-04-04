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
    materialized = 'table'
) -}}
-- depends_on: {{ ref('unioned_thresholds') }}
-- depends_on: {{ ref('metric') }}
-- depends_on: {{ ref('metric_definition') }}

WITH A AS (

{%- set time_grains = dbt_utils.get_column_values(table=ref('unioned_thresholds'), column='time_grain') -%}
{%- for grain in time_grains %}
  {%- if grain != None %}
    {% set grain_filter_expression = "time_grain = '" ~ grain ~ "'" %}
  {%- else %}
    {%- set grain_filter_expression = "time_grain IS NULL" %}
  {%- endif %}
  {%- set dimensions = dbt_utils.get_column_values(table=ref('unioned_thresholds'), column='dimension', where=grain_filter_expression) %}
  {%- for dimension in dimensions %}
SELECT
  T.metric_name,
  D.description,
  D.primary_resource,
  D.calculation,
  D.category,
  T.time_grain,
  T.dimension,
  T.validation_feature,
  T.severity,
  T.threshold_low,
  T.threshold_high,
  {% if grain == None or grain == ""  %} '' {% else -%} {{ date_to_period('M.metric_date', grain) }} {% endif -%} as metric_date,
  {% if dimension == None or dimension == "" %} '' {% else -%} D.dimension_{{dimension}} {% endif -%} as dimension_name,
  {% if dimension == None or dimension == "" %} '' {% else -%} M.dimension_{{dimension}} {% endif -%} as dimension_value,
  SUM(M.numerator) AS numerator,
  SUM(M.denominator) AS denominator,
  {{ calculate_measure() }} measure
FROM {{ ref('unioned_thresholds') }} T
INNER JOIN {{ ref('metric') }} M ON T.metric_name = M.metric_name
INNER JOIN {{ ref('metric_definition') }} D ON T.metric_name = D.metric_name
WHERE D.calculation IN ('COUNT','PROPORTION')
  AND T.time_grain {% if grain == None -%} IS NULL {% else -%} = '{{ grain }}' {% endif %}
  AND T.dimension {% if dimension == None -%} IS NULL {% else -%} = '{{ dimension }}' {% endif %}
{{ dbt_utils.group_by(14)|upper }}
    {% if not loop.last -%}  UNION ALL {%- endif -%}
  {%- endfor -%}
  {% if not loop.last -%}  UNION ALL {%- endif -%}
{%- endfor -%}

)

SELECT
  *,
  CASE
    WHEN measure IS NULL THEN 'N/A'
    WHEN measure < threshold_low THEN severity
    WHEN measure > threshold_high THEN severity
    ELSE 'Pass'
    END AS status
FROM A
