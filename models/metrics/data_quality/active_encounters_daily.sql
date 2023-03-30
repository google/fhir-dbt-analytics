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
      "description": "Count of inpatient encounters that are active at any time during each day",
      "short_description": "Active encounters",
      "primary_resource": "Encounter",
      "primary_fields": ['period.start', 'period.end'],
      "secondary_resources": [],
      "calculation": "COUNT",
      "category": "Encounter daily",
      "dimension_a": "encounter_class",
      "dimension_a_description": "The latest class of the encounter",
      "dimension_b": "encounter_service_provider",
      "dimension_b_description": "The organization responsible for providing the services for this encounter",
    }
) -}}

{%- set metric_sql -%}
    {{ active_encounters() }}
{%- endset -%}

{{- calculate_metric(metric_sql) -}}