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
  {{ date_add_days(period_start, length_of_stay_cap) }},
  IF({{encounter_class}} = 'AMB', {{period_start}}, CURRENT_DATE())
)
{%- endmacro -%}