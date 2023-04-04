{% macro maybe_drop_metric_tables() %}
    {%- if not var('drop_metric_tables') or not execute -%}
       {% do return('') %}
    {%- endif -%}

    {%- for metric_table in get_metric_tables().values() %}
       {{ run_query("DROP TABLE "~metric_table) }}
    {%- endfor -%}

    {{ return("") }}
{% endmacro %}