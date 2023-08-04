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

{% macro timestamp_trunc(datepart, date, timezone=None) -%}
  {{ return(adapter.dispatch('timestamp_trunc', 'fhir_dbt_analytics') (datepart, date, timezone)) }}
{%- endmacro %}


{% macro bigquery__timestamp_trunc(datepart, date, timezone) -%}
  TIMESTAMP_TRUNC({{ date }}, {{ datepart }}, {{ timezone }})
{%- endmacro %}


{% macro default__timestamp_trunc(datepart, date, timezone) -%}
  {{ dbt.date_trunc(datepart, date) }}
{%- endmacro %}