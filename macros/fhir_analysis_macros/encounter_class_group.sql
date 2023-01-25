{%- macro encounter_class_group(class_code) -%}
CASE
  WHEN UPPER({{class_code}}) = 'AMB' THEN 'Ambulatory'
  WHEN UPPER({{class_code}}) IN ('IMP', 'ACUTE', 'NONAC', 'SS', 'OBSENC', 'EMER') THEN 'Non-Ambulatory'
  ELSE 'Other' END
{%- endmacro -%}