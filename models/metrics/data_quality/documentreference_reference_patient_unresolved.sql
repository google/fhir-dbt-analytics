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
      "description": "Proportion of DocumentReference resources that reference a non-existent patient",
      "short_description": "DocRef ref. Patient - non-exist",
      "primary_resource": "DocumentReference",
      "primary_fields": ['subject.patientId'],
      "secondary_resources": ['Patient'],
      "calculation": "PROPORTION",
      "category": "Referential integrity",
      "metric_date_field": "DocumentReference.date",
      "metric_date_description": "Document reference date",
      "dimension_a": "document_status",
      "dimension_a_description": "The document reference status  (current | superseded | entered-in-error)",
      "dimension_b": "format",
      "dimension_b_description": "The format of the document referenced (scanned | NULL)",
    }
) -}}

-- depends_on: {{ ref('Patient') }}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status as document_status,
      {%- if fhir_dbt_utils.field_exists('content.format.code') %}
      {{ fhir_dbt_utils.safe_offset("content", 0) }}.format.code AS format
      {%- else %}
      NULL AS format
      {%- endif %},
      {{ has_reference_value('subject', 'Patient') }} AS has_reference_value,
      {{ reference_resolves('subject', 'Patient') }} AS reference_resolves
    FROM {{ ref('DocumentReference') }} AS D
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = 'SUM(has_reference_value - reference_resolves)',
    denominator = 'COUNT(id)'
) }}
