{%- macro metric_date(metric_date_columns, date_column_data_type) -%}
{%- if metric_date_columns == None %}
    CAST(NULL AS DATE)
{%- else -%}
    {%- if metric_date_columns | length > 1 -%}
      {%- set date_column = "COALESCE(" + metric_date_columns|join(", ") + ")" -%}
    {%- else %}
      {%- set date_column = metric_date_columns[0] -%}
    {%- endif -%}
      {{ local_date(date_column, date_column_data_type) }}
{%- endif -%}
{%- endmacro -%}