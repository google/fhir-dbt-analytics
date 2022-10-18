{% macro union_metric_tables() -%}
    {%- set metric_tables -%}
        {{ get_metric_tables() }}
    {%- endset -%}
    {%- if execute -%}
        {%- set metrics = run_query(metric_tables).columns[0].values() -%}
        {%- for metric in metrics -%}
            {%- set relation = adapter.get_relation(
                  database = target.project,
                  schema = target.dataset,
                  identifier = metric
            ) %}
            SELECT * FROM {{ relation.database }}.{{ relation.schema }}.{{ relation.name }}
            {% if not loop.last -%}  UNION ALL {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
{%- endmacro -%}