{% macro active_encounters() %}
  SELECT *
  FROM Anchor
  JOIN Enc
    ON IF (
      encounter_class = 'Ambulatory',
      Enc.encounter_start_date = Anchor.metric_date,
      Enc.encounter_start_date <= Anchor.metric_date
        AND (
          Enc.encounter_end_date >= Anchor.metric_date
          OR Enc.encounter_end_date IS NULL
        )
        AND DATE_DIFF(
          CASE WHEN encounter_end_date > CURRENT_DATE() OR encounter_end_date IS NULL THEN CURRENT_DATE() ELSE encounter_end_date END,
          encounter_start_date,
          DAY
        ) < 90
    )
{% endmacro %}