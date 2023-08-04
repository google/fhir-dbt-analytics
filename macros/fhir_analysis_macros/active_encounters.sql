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

{%- macro active_encounters(encounter_classes=['IMP', 'ACUTE', 'NONAC', 'SS', 'OBSENC', 'EMER', 'AMB']) %}
      WITH
        Enc AS (
          SELECT
            id,
            subject.patientId AS patientId,
            {{ metric_date(['period.start'])|indent(2) }} AS period_start,
            {{ metric_date(['period.end'])|indent(2) }} AS period_end,
            {{- metric_common_dimensions(exclude_col='metric_date')|indent }}
            CASE WHEN UPPER(class.code) IN ('IMP', 'ACUTE', 'NONAC') THEN 'IMP/ACUTE/NONAC' ELSE class.code END AS encounter_class,
            {{ encounter_class_group('class.code')|indent(6) }} AS encounter_class_group,
            {{ get_column_or_default('serviceProvider.organizationId', 'Encounter') }} AS encounter_service_provider
          FROM {{ ref('Encounter') }}
          WHERE
            UPPER(class.code) {{ sql_comparison_expression(encounter_classes) }}
            AND status IN ('in-progress', 'finished')
            AND period.start IS NOT NULL
            AND period.start <> ''
        ),
        DS AS (
          {{ date_spine()|indent }}
        )

        SELECT
          Enc.*,
          DS.date_day AS metric_date
        FROM Enc JOIN DS
          ON DS.date_day BETWEEN Enc.period_start
            AND {{ cap_encounter_end_date()|indent }}
{%- endmacro -%}