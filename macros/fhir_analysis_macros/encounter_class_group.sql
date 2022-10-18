{% macro encounter_class_group(class_code) %}
  CASE
    WHEN {{class_code}} = 'AMB' THEN 'Ambulatory'
    WHEN {{class_code}} IN ('EMER', 'IMP', 'ACUTE', 'NONAC', 'OBSENC', 'SS') THEN 'Non-Ambulatory'
    ELSE 'Not defined' END
{% endmacro %}