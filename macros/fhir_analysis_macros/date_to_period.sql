{% macro date_to_period(date_column, date_period) -%}
{%- if date_period == 'YEAR' -%}
 {%- set format_string = '%Y' -%}
{%- elif date_period == 'QUARTER' -%}
 {%- set format_string  = '%Y Q%Q' -%}
{%- elif date_period == 'MONTH' -%}
 {%- set format_string = '%Y M%m' -%}
{%- else -%}
 {%- set format_string = '%F' -%}
{%- endif -%}
FORMAT_DATE('{{format_string}}', {{date_column}})
{%- endmacro -%}