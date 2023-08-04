-- Copyright 2023 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

{% macro date_spine() -%}
  {{ return(adapter.dispatch('date_spine2', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}


{% macro default__date_spine2() -%}

{%- if var('static_dataset') -%}
  {% set start_date="cast('" ~ var('earliest_date') ~ "' as date)" %}
  {% set end_date="cast('" ~ var('latest_date') ~ "' as date)" %}
{%- else -%}
  {% set start_date=dbt_date.n_months_ago(var('months_history'), tz=data_timezone()) %}
  {% set end_date=dbt_date.today(tz=data_timezone()) %}
{%- endif -%}

-- ---------------------- start of date_spine -----------------------
with date_spine as (
  {{ dbt_utils.date_spine(datepart="day", start_date=start_date, end_date=end_date) }}
)
{# dbt_utils cast to datetime, but we want date #}
select cast(date_day as date) as date_day from date_spine
-- ---------------------- end of date_spine -------------------------
{% endmacro %}


{% macro bigquery__date_spine2() -%}
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(
{%- if var('static_dataset') %}
  "{{ var('earliest_date') }}", "{{ var('latest_date') }}"
{%- else %}
  DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('months_history') }} MONTH),
  CURRENT_DATE()
{%- endif %}
)) AS date_day
{%- endmacro %}
