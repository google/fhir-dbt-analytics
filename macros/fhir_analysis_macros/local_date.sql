{% macro local_date(date_column, date_column_data_type) -%}
{%- if date_column_data_type == 'TIMESTAMP' -%}
    {{ date(
      date_column,
      data_timezone())|indent(6) }}
{%- else -%}
    IF(
      CHAR_LENGTH({{ date_column }}) = 10,
      {{ safe_cast_as_date(date_column) }},
      {{ date(
        safe_cast_as_timestamp(date_column),
        data_timezone())|indent(8) }}
    )
{%- endif -%}
{%- endmacro -%}