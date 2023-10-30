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

{# TODO(jakuba): Explore if we could get table info using SHOW TABLES and processing the result by
   dbt. #}
{% if is_spark() %}
  {%- set path_prefix='fhir_resources/' %}
  {%- set fhir_resources={} %}
  {%- for node in graph.nodes.values()
       if node.resource_type == 'model'
       and node.path.startswith(path_prefix)
       and not node.path.endswith('_view.sql') %}
    {%- set fhir_resource = node.path[path_prefix|length : -4] %}
    {% if var('snake_case_fhir_tables') %}
        {%- set table_name = snake_case(fhir_resource) %}
    {% else %}
        {%- set table_name = fhir_resource %}
    {% endif %}
    {%- if adapter.get_relation(
      database = var('database'),
      schema = var('schema'),
      identifier = table_name) %}
      {%- do fhir_resources.update({fhir_resource: table_name}) %}
    {%- endif %}
  {%- endfor %}

  {%- for fhir_resource, table_name in fhir_resources.items() %}
SELECT
  '{{ fhir_resource }}' AS fhir_resource,
  NULL as bq_project,
  NULL as bq_dataset,
  '{{ table_name }}' as bq_table,
  '`{{ var('schema') }}`.`{{ table_name }}`' AS fully_qualified_bq_table,
  NULL AS map_name,
  1 as latest_version,
  NULL AS creation_time
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}

{% else %} {# end of is_spark() #}

SELECT
  table_catalog as bq_project,
  table_schema as bq_dataset,
  table_name as bq_table,
  CONCAT('`', table_catalog, '`.`', table_schema, '`.`', table_name, '`') AS fully_qualified_bq_table,
  {%- if var('multiple_tables_per_resource') -%}
  SPLIT(table_name, '_')[SAFE_OFFSET(0)] AS fhir_resource,
  REGEXP_REPLACE(table_name, r'(_[0-9]{8,})+.*', '') AS map_name,
  IF(1 = ROW_NUMBER() OVER (
    PARTITION BY REGEXP_REPLACE(table_name, r'(_[0-9]{8,})+.*', '')
    ORDER BY
      IF(table_name LIKE '%golden', 1, 0) DESC,
      creation_time DESC
  ), 1, 0) AS latest_version,
  {%- else %}
  REPLACE(INITCAP(table_name), '_', '') AS fhir_resource,
  NULL AS map_name,
  1 AS latest_version,
  {%- endif -%}
  creation_time
FROM `{{ var('database') }}`.`{{ var('schema') }}`.INFORMATION_SCHEMA.TABLES

{% endif %}