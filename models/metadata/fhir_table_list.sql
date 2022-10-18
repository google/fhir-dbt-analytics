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
  table_catalog as bq_project,
  table_schema as bq_dataset,
  table_name as bq_table,
  CONCAT('`', table_catalog, '`.`', table_schema, '`.`', table_name, '`') AS fully_qualified_bq_table,
  table_name AS fhir_resource,
  NULL AS map_name,
  1 AS latest_version,
  creation_time
FROM {{ var('database') }}.{{ var('schema') }}.INFORMATION_SCHEMA.TABLES