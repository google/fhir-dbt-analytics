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
      "description": "Count of valid ServiceRequest resources",
      "short_description": "ServiceRequest resources",
      "primary_resource": "ServiceRequest",
      "primary_fields": ['id'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Resource count",
      "metric_date_field": "ServiceRequest.authoredOn",
      "metric_date_description": "Service request signed date",
      "dimension_a": "status",
      "dimension_a_description": "The status of the service request (draft | active | on-hold | revoked | completed | entered-in-error | unknown)",
      "dimension_b": "intent",
      "dimension_b_description": "The intent of the service request (proposal | plan | directive | order | original-order | reflex-order | filler-order | instance-order | option)",
      "dimension_c": "category",
      "dimension_c_description": "The category of the service request",
    }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      status,
      intent,
      {{ fhir_dbt_utils.code_from_codeableconcept('category', 'http://snomed.info/sct', return_field='display') }} AS category
    FROM {{ ref('ServiceRequest') }}
{%- endset -%}

{{ calculate_metric(metric_sql) }}