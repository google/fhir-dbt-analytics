{% macro date_to_period(date_column, date_period) -%}
  {{ return(adapter.dispatch('date_to_period', 'fhir_dbt_analytics') (date_column, date_period)) }}
{%- endmacro %}


{% macro default__date_to_period(date_column, date_period) -%}
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
{%- endmacro %}


{% macro spark__date_to_period(date_column, date_period) -%}
{%- if date_period == 'YEAR' -%}
 {%- set format_string = 'y' -%}
{%- elif date_period == 'QUARTER' -%}
 {%- set format_string  = "y qqq" -%}
{%- elif date_period == 'MONTH' -%}
 {%- set format_string = "y 'M'M" -%}
{%- else -%}
 {%- set format_string = 'yMMdd' -%}
{%- endif -%}
DATE_FORMAT({{date_column}}, '{{format_string}}')
{%- endmacro %}
