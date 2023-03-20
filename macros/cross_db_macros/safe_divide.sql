{%- macro safe_divide(x, y) -%}
IF(({{ y }}) != 0, ({{ x }}) / ({{ y }}), NULL)
{%- endmacro -%}