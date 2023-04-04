{% macro union_metric_tables() -%}
  {%- if execute -%}
    {%- for metric_table in get_metric_tables().values() %}
      SELECT * FROM {{ metric_table }}
      {% if not loop.last -%}UNION ALL{% endif %}
    {%- endfor -%}
  {%- endif -%}
{%- endmacro -%}