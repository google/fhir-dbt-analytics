{%- macro metric_hour(metric_date_columns, date_column_data_type) -%}
{%- if metric_date_columns == None %}
    CAST(NULL AS TIMESTAMP)
{%- else -%}
    {%- if metric_date_columns | length > 1 -%}
      {%- set date_column = "COALESCE(" + metric_date_columns|join(", ") + ")" -%}
    {%- else %}
      {%- set date_column = metric_date_columns[0] -%}
    {%- endif -%}
    {{ local_hour(date_column, date_column_data_type) }}
{%- endif -%}
{%- endmacro -%}