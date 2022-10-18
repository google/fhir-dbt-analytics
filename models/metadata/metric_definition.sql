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

{% for node_id, node in graph.nodes.items() if node.resource_type == 'model' and node.path.startswith('metrics/') %}
SELECT
    "{{ node.path }}" AS metric_path,
    "{{ node.name }}" AS metric_name,
    "{{ node.config.meta.description }}" AS description,
    "{{ node.config.meta.short_description }}" AS short_description,
    "{{ node.config.meta.primary_resource }}" AS primary_resource,
    {{ node.config.meta.primary_fields }} AS primary_fields,
    {{ node.config.meta.secondary_resources }} AS secondary_resources,
    "{{ node.config.meta.category }}" AS category,
    "{{ node.config.meta.calculation }}" AS calculation,
    "{{ node.config.meta.metric_date_field }}" AS metric_date_field,
    "{{ node.config.meta.metric_date_description }}" AS metric_date_description,
    "{{ node.config.meta.dimension_a_name }}" AS dimension_a_name,
    "{{ node.config.meta.dimension_a_description }}" AS dimension_a_description,
    "{{ node.config.meta.dimension_b_name }}" AS dimension_b_name,
    "{{ node.config.meta.dimension_b_description }}" AS dimension_b_description,
    "{{ node.config.meta.dimension_c_name }}" AS dimension_c_name,
    "{{ node.config.meta.dimension_c_description }}" AS dimension_c_description
{% if not loop.last -%} UNION ALL {%- endif %}
{%- endfor -%}