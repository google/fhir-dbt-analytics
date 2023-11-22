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

{% for node in graph.nodes.values() if node.resource_type == 'model' and node.path.startswith('metrics/') %}
SELECT
    "{{ node.path }}" AS metric_path,
    "{{ node.name }}" AS metric_name,
    "{{ node.config.meta.description }}" AS description,
    "{{ node.config.meta.short_description }}" AS short_description,
    "{{ node.config.meta.primary_resource }}" AS primary_resource,
    {{ dbt.array_construct(fhir_dbt_utils.quote_array(node.config.meta.primary_fields)) }} AS primary_fields,
    {{ dbt.array_construct(fhir_dbt_utils.quote_array(node.config.meta.secondary_resources))
       if node.config.meta.secondary_resources else 'NULL' }} AS secondary_resources,
    "{{ node.config.meta.category }}" AS category,
    "{{ node.config.meta.calculation }}" AS calculation,
    "{{ node.config.meta.metric_date_field }}" AS metric_date_field,
    "{{ node.config.meta.metric_date_description }}" AS metric_date_description,
    "{{ node.config.meta.dimension_a }}" AS dimension_a,
    "{{ node.config.meta.dimension_a_description }}" AS dimension_a_description,
    "{{ node.config.meta.dimension_b }}" AS dimension_b,
    "{{ node.config.meta.dimension_b_description }}" AS dimension_b_description,
    "{{ node.config.meta.dimension_c }}" AS dimension_c,
    "{{ node.config.meta.dimension_c_description }}" AS dimension_c_description
{% if not loop.last -%} UNION ALL {%- endif %}
{%- endfor -%}