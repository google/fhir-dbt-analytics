{% macro has_code_from_code_system(field_name, code_system) %}
  (
    SELECT SIGN(COUNT(*))
    FROM UNNEST({{field_name}}.coding) AS cc
    WHERE cc.system = '{{code_system}}'
    AND cc.code IS NOT NULL
    AND cc.code <> ''
  )
{% endmacro %}