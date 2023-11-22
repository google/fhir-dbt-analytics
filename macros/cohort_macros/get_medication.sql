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

{% macro get_medication(codeable_concept_field, code_system = None)-%}

  {%- if fhir_dbt_utils.field_exists('medication.reference.medicationid') and fhir_dbt_utils.field_exists('medication.codeableConcept')%}

    {%- if codeable_concept_field == 'text' %}
    IF(medication.reference.medicationid IS NOT NULL,
      m.code.text,
      medication.codeableConcept.text
    )
    {% elif codeable_concept_field in ('code' , 'display') and code_system != None %}
    IF(medication.reference.medicationid IS NOT NULL, 
      ( SELECT cc.{{codeable_concept_field}}
          FROM UNNEST(m.code.coding) AS cc
          WHERE cc.system= {{ code_system }} )
      ,
      (SELECT cc.{{codeable_concept_field}}
       FROM UNNEST(medication.codeableConcept.coding) AS cc
       WHERE cc.system={{ code_system }} )
      )
    {%- endif %}

  {%- elif fhir_dbt_utils.field_exists('medication.codeableConcept') %}

    {%- if codeable_concept_field == 'text' %}
      medication.codeableConcept.text
    {% elif codeable_concept_field in ('code' , 'display') and code_system != None %}
      (SELECT cc.{{codeable_concept_field}}
       FROM UNNEST(medication.codeableConcept.coding) AS cc 
       WHERE cc.system={{ code_system }} )
    {%- endif %}
  {%- else %}
    {%- if codeable_concept_field == 'text' %}
      m.code.text
    {% elif codeable_concept_field in ('code' , 'display') and code_system != None %}
      ( SELECT cc.{{codeable_concept_field}}
          FROM UNNEST(m.code.coding) AS cc
          WHERE cc.system= {{ code_system }} )
    {%- endif %}

  {%- endif %}

{%- endmacro %}