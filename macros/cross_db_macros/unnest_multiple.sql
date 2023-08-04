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

{% macro unnest_multiple(arrays) -%}
  {{ return (adapter.dispatch('unnest_multiple', 'fhir_dbt_analytics') (arrays)) }}
{%- endmacro %}


{% macro array_config(field, unnested_alias = 'unused') %}
  {{ return ({ "field": field, "unnested_alias": unnested_alias}) }}
{% endmacro %}


{% macro default__unnest_multiple(arrays) -%}
  {% for array in arrays -%}
    UNNEST({{ array.field }}) {{ array.unnested_alias }}
    {%- if not loop.last -%}, {% endif -%}
  {%- endfor -%}
{%- endmacro -%}


{% macro spark__unnest_multiple(arrays) -%}
  {# Select the field as `ac`: this "cheeky select" trick allows us to write the unnest as one
      statement. #}
  {%- set array0 = arrays[0] -%}
    SELECT * FROM (SELECT EXPLODE(ac) AS {{ array0.unnested_alias }} FROM (SELECT {{ array0.field }} AS ac))
  {%- for array in arrays[1:] %}
    LATERAL VIEW OUTER explode ({{ array.field }}) AS {{ array.unnested_alias }}
  {%- endfor -%}
{%- endmacro %}
