{% macro has_code_from_code_system(field_name, code_system, null_values=var('null_values')) %}
      (
        SELECT SIGN(COUNT(*))
        FROM UNNEST({{field_name}}.coding) AS cc
        WHERE cc.system = '{{code_system}}'
        AND cc.code IS NOT NULL
        {%- if null_values|length > 0 %}
        AND cc.code NOT IN ('{{null_values|join("', '")}}')
        {%- endif %}
      )
{%- endmacro -%}