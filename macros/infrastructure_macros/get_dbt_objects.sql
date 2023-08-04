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

{% macro get_dbt_objects(resource_type=None) -%}
{%- set models_dict = {} -%}
{% if execute %}
  {% if resource_type!=None %}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", resource_type) %}
      {% do models_dict.update({node.name: node.path}) %}
    {% endfor %}
  {% else %}
    {% for node in graph.nodes.values() %}
      {% do models_dict.update({node.name: node.path}) %}
    {% endfor %}
  {% endif %}
{% endif %}
{% do return(models_dict) %}
{%- endmacro -%}
