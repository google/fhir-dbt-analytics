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

{% macro get_reference_paths(
  reference_column,
  reference_resource
) %}

{#- For a reference_resource FooBar, direct reference is fooBarId. -#}
{%- set direct_reference = reference_resource[:1]|lower ~ reference_resource[1:] ~ 'Id' -%}

{#- For a reference_resource FooBar and reference_column foo, direct reference path is foo.fooBarId. -#}
{%- set direct_reference_path = reference_column ~ '.' ~ direct_reference -%}

{#- For a reference_column foo, indirect reference path is foo.reference. -#}
{%- set indirect_reference_path = reference_column ~ '.reference' -%}

{%- set fhir_resource = model_metadata(meta_key='fhir_resource') -%}
{%- set column_dict = get_column_datatype_dict(fhir_resource) -%}

{%- set reference_column_is_array = 
      execute 
      and reference_column in column_dict 
      and column_dict[reference_column].startswith('ARRAY')
-%}

{%- do return({
      "direct_reference": direct_reference,
      "direct_reference_path": direct_reference_path,
      "indirect_reference_path": indirect_reference_path,
      "reference_column_is_array": reference_column_is_array
}) -%}

{%- endmacro -%}