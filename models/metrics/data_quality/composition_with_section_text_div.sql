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

{{ config(
    meta = {
      "description": "Proportion of Composition resources that contain a non-empty section[].text.div",
      "short_description": "Comp with section div",
      "primary_resource": "Composition",
      "primary_fields": ['section.text.div'],
      "secondary_resources": [],
      "calculation": "PROPORTION",
      "category": "Data completeness",
      "metric_date_field": "date",
      "metric_date_description": "Composition date",
      "dimension_a": "composition_status",
      "dimension_a_description": "The composition status  (preliminary | final | amended | entered-in-error)",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status as composition_status,
      {%- if fhir_dbt_utils.field_exists('section.text.div') %}
      {{ fhir_dbt_utils.safe_offset("section", 0) }}.text.div IS NOT NULL
        AND {{ fhir_dbt_utils.safe_offset("section", 0) }}.text.div <> '' AS has_section_text_div
      {%- else %}
      FALSE AS has_section_text_div
      {%- endif %},
    FROM {{ ref('Composition') }} AS C
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(CAST(has_section_text_div AS '~fhir_dbt_utils.type_long()~'))',
    denominator = 'COUNT(id)'
) }}