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
  '{{ var('source_system_default') }}' AS `source_system_default`,
  '{{ var('site_default') }}' AS `site_default`,
  '{{ var('data_transfer_type_default') }}' AS `data_transfer_type_default`,
  '{{ var('timezone_default') }}' AS `timezone_default`,
  {{ var('static_dataset') }} AS `static_dataset`,
  '{{ var('earliest_date') }}' AS `earliest_date`,
  '{{ var('latest_date') }}' AS `latest_date`,
  {{ var('months_history') }} AS `months_history`,
  {{ var('length_of_stay_cap') }} AS `length_of_stay_cap`,
  {{ var('null_values') }} AS `null_values`,
  {{ var('persist_all_metric_executions') }} AS `persist_all_metric_executions`,
  '{{ var('cohort') }}' AS `cohort`,
  '{{ var('cohort_snapshot_date') }}' AS `cohort_snapshot_date`,
  '{{ var('drop_metric_tables') }}' AS `drop_metric_tables`,
  {{ linking_api_datasource(
    report_id='2de086de-b4ab-4431-8786-57ffa47e96b8',
    datasource_list=[
      'metric_by_slices_system_date',
      'metric_by_system_date',
      'metric_by_system',
      'metric_definition',
      'fhir_table_list',
      'project_variables',
      'metric_execution_log'
    ]) }} AS looker_studio_url