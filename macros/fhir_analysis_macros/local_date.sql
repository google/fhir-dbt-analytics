{% macro local_date(date_column, date_column_data_type) -%}
{%- if date_column_data_type == 'TIMESTAMP' -%}
    DATE(
      {{ date_column }},
      {{ data_timezone()|indent(6) }}
    )
{%- else -%}
    IF(
      CHAR_LENGTH({{ date_column }}) = 10,
      SAFE_CAST({{ date_column }} AS DATE),
      DATE(
        SAFE_CAST({{ date_column }} AS TIMESTAMP),
        {{- data_timezone()|indent(8) }}
      )
    )
{%- endif -%}
{%- endmacro -%}