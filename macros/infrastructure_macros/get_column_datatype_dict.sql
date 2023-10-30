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

{% macro get_column_datatype_dict(fhir_resource=None) %}

    {%- if not execute -%}
        {% do return({}) %}
    {% endif %}

    {%- if fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='fhir_resource') -%}
    {%- endif -%}
    {%- if fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='primary_resource') -%}
    {%- endif -%}

    {%- set relation = adapter.get_relation(
        database = this.project,
        schema = this.schema,
        identifier = fhir_resource) -%}

    {% if not relation %}
        {% do return ({}) %}
    {% endif %}

    {%- set column_dict = {} -%}

    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {% for top_level_column in columns %}
        {%- do column_dict.update({top_level_column.name: top_level_column.data_type}) -%}
        {% for column in flatten_column(top_level_column) %}
            {%- do column_dict.update({column.name: column.data_type}) -%}
        {% endfor %}
    {%- endfor -%}

    {%- do return(column_dict) -%}

{% endmacro %}