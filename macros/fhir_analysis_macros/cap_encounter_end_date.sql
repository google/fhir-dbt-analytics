{%- macro cap_encounter_end_date(
  period_start='period_start',
  period_end='period_end',
  encounter_class='encounter_class',
  length_of_stay_cap=None
) -%}
{%- if length_of_stay_cap == None -%}
{%- set length_of_stay_cap = var('length_of_stay_cap') -%}
{%- endif -%}
LEAST(
  IFNULL({{period_end}}, CURRENT_DATE()),
  DATE_ADD({{period_start}}, INTERVAL {{length_of_stay_cap}} DAY),
  IF({{encounter_class}} = 'AMB', {{period_start}}, CURRENT_DATE())
)
{%- endmacro -%}