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

{% macro get_metric_tables() %}
  {{ return(adapter.dispatch('get_metric_tables', 'fhir_dbt_analytics') ()) }}
{% endmacro %}


{% macro bigquery__get_metric_tables() %}
  {% set metric_tables %}
  SELECT T.table_name
  FROM {{target.project}}.{{target.dataset}}.INFORMATION_SCHEMA.TABLES AS T
  JOIN {{ ref('metric_all_definitions') }} AS D ON T.table_name = D.metric_name
  {% endset %}

  {% set metric_names = run_query(metric_tables).columns[0].values() %}
  {% set metric_dict = {} %}
  {% for metric in metric_names %}
    {{ metric_dict.update({ metric : table_ref(target.project, target.schema, metric) } )}}
  {% endfor %}

  {{ return(metric_dict) }}
{% endmacro %}


{% macro spark__get_metric_tables() %}
  {% if not execute %}
    {{ return (None) }}
  {% endif %}
  {% set show_tables %}
    SHOW TABLES IN {{target.schema}}
  {% endset %}
  {% set metrics = run_query(show_tables).columns[1].values() %}
  {% set metric_dict = {} %}
  {% for node in graph.nodes.values()
     if node.resource_type == 'model'
     and node.path.startswith('metrics/')
     and node.name in metrics %}
     {{ metric_dict.update({ node.name : table_ref("", target.schema, node.name) }) }}
  {% endfor %}
  {{ return (metric_dict) }}
{% endmacro %}
