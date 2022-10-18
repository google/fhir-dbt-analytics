{% macro sql_comparison_expression(code_string_or_list) -%}
 {%- if code_string_or_list is string -%}
  = '{{ code_string_or_list }}'
  {%- elif code_string_or_list is iterable -%}
  IN ('{{ code_string_or_list|join("', '") }}')
  {%- endif -%}
{% endmacro %}