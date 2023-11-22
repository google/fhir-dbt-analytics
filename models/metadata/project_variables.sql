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

SELECT
  '{{ var('organization') }}' AS `organization`,
  '{{ var('database') }}' AS `database`,
  '{{ var('schema') }}' AS `schema`,
  '{{ var('fhir_version') }}' AS `fhir_version`,
  {{ var('snake_case_fhir_tables') }} AS `snake_case_fhir_tables`,
  {{ var('multiple_tables_per_resource') }} AS `multiple_tables_per_resource`,
  {{ var('assume_resources_exist') }} AS `assume_resources_exist`,
  {{ var('assume_fields_exist') }} AS `assume_fields_exist`,
  '{{ var('source_system_default') }}' AS `source_system_default`,
  '{{ var('site_default') }}' AS `site_default`,
  '{{ var('timezone_default') }}' AS `timezone_default`,
  {{ var('length_of_stay_cap') }} AS `length_of_stay_cap`,
  {{ dbt.array_construct(fhir_dbt_utils.quote_array(var('null_values'))) }} AS `null_values`,
  {{ var('static_dataset') }} AS `static_dataset`,
  '{{ var('earliest_date') }}' AS `earliest_date`,
  '{{ var('latest_date') }}' AS `latest_date`,
  {{ var('months_history') }} AS `months_history`,
  {{ var('persist_all_metric_executions') }} AS `persist_all_metric_executions`,
  {{ var('drop_metric_tables') }} AS `drop_metric_tables`,
  {{ var('print_why_metric_empty') }} AS `print_why_metric_empty`,
  '{{ var('init_sources_fhir_resource_list') }}' AS `init_sources_fhir_resource_list`,
  '{{ var('init_sources_parquet_location') }}' AS `init_sources_parquet_location`,
  {{ var('patient_panel_enabled') }} AS `patient_panel_enabled`,
  {{ var('encounter_lookback_years') }} AS `encounter_lookback_years`,
  {{ var('minimum_age') }} AS `minimum_age`,
  '{{ var('age_calculation_method') }}' AS `age_calculation_method`,
  {{ var('empi_as_person_enabled') }} AS `empi_as_person_enabled`,
  '{{ var('cohort_snapshot_date') }}' AS `cohort_snapshot_date`,
  {{ linking_api_datasource(
    report_id='2de086de-b4ab-4431-8786-57ffa47e96b8',
    datasource_list=[
      ['mbdd', 'metric_by_day_dimensions'],
      ['mbd', 'metric_by_day'],
      ['mo', 'metric_overall'],
      ['md', 'metric_definition'],
      ['ftl', 'fhir_table_list'],
      ['pv', 'project_variables'],
      ['mel', 'metric_execution_log'],
      ['mt', 'metric_thresholds']
    ]) }} AS looker_studio_url
