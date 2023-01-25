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
            serviceProvider.organizationId AS encounter_service_provider,
            {{ date_array()|indent(6) }} as date_array
          FROM {{ ref('Encounter') }}
          WHERE
            UPPER(class.code) {{ sql_comparison_expression(encounter_classes) }}
            AND status IN ('in-progress', 'finished')
            AND period.start IS NOT NULL
            AND period.start <> ''
        )
        SELECT *
        FROM Enc
        JOIN UNNEST(date_array) as metric_date
          ON Enc.period_start <= metric_date
          AND {{ cap_encounter_end_date()|indent }} >= metric_date
{%- endmacro -%}