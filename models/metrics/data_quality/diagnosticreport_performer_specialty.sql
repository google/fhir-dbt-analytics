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
      "description": "Proportion of diagnostic reports referencing a practioner role with specialty recorded",
      "short_description": "DiagRep prac. specialty recorded",
      "primary_resource": "DiagnosticReport",
      "primary_fields": ['performer'],
      "secondary_resources": ['Practitioner', 'PractitionerRole'],
      "calculation": "PROPORTION",
      "category": "Data completeness",
      "metric_date_field": "DiagnosticReport.issued",
      "metric_date_description": "Diagnostic report latest version issue date",
      "dimension_a": "category",
      "dimension_a_description": "The service category of the diagnostic report",
    }
) -}}

{%- set metric_sql -%}

  SELECT
    D.id,
    {{- metric_common_dimensions("D") }}
    {{ code_from_codeableconcept(
      'category',
      'https://g.co/fhir/harmonized/diagnostic_report/category',
      index = get_source_specific_category_index()
    ) }} AS category,
    (
      SELECT SIGN(COUNT(*))
      FROM UNNEST(PR.specialty) AS s
      WHERE s.coding IS NOT NULL
        OR (s.text IS NOT NULL AND s.text <> '')
    ) AS has_specialty
  FROM {{ ref('DiagnosticReport') }} AS D
  LEFT JOIN UNNEST(D.performer) AS Dp
  LEFT JOIN {{ ref('Practitioner') }} AS P
    ON Dp.practitionerId = P.id
  LEFT JOIN {{ ref('PractitionerRole') }} PR
    ON P.id = PR.practitioner.practitionerid

{%- endset -%}

{{- calculate_metric(
    metric_sql,
    numerator = 'COUNT(DISTINCT IF(has_specialty = 1, id, NULL))',
    denominator = 'COUNT(DISTINCT id)'
) -}}
