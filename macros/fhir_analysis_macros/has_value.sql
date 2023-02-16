{% macro has_value(field_name, null_values=var('null_values')) -%}

{{field_name}} IS NOT NULL {%- if null_values|length > 0 %} AND {{field_name}} NOT IN ('{{null_values|join("', '")}}') {%- endif -%}

{%- endmacro -%}