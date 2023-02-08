{% macro union_metric_tables() -%}
    {%- set metric_tables -%}
        {{ get_metric_tables() }}
    {%- endset -%}
    {%- if execute -%}
        {%- set metrics = run_query(metric_tables).columns[0].values() -%}
        SELECT * FROM ({{ empty_metric_output() }})
        WHERE 1=0
        {%- for metric in metrics -%}
            {%- set relation = adapter.get_relation(
                  database = target.project,
                  schema = target.dataset,
                  identifier = metric
            ) %}
            UNION ALL
            SELECT * FROM {{ relation.database }}.{{ relation.schema }}.{{ relation.name }}
        {%- endfor -%}
    {%- endif -%}
{%- endmacro -%}