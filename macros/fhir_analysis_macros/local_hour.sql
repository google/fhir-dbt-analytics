{% macro local_hour(date_column, date_column_data_type) -%}
{%- if date_column_data_type == 'TIMESTAMP' -%}
    TIMESTAMP_TRUNC(
      {{ date_column }}, 
      HOUR,
      {{ data_timezone()|indent(6) }}
    )

{%- else -%}
    IF(
      CHAR_LENGTH({{ date_column }}) = 10,
      SAFE_CAST(NULL AS TIMESTAMP),
      TIMESTAMP_TRUNC(
        SAFE_CAST({{ date_column }} AS TIMESTAMP),
        HOUR,
        {{- data_timezone()|indent(8) }}
      )
    )
{%- endif -%}
{%- endmacro -%}