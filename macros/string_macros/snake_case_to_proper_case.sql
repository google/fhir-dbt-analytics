{%- macro snake_case_to_proper_case(snake_case_column) -%}
REPLACE(INITCAP({{ snake_case_column }}, '_'), '_', ' ')
{%- endmacro -%}