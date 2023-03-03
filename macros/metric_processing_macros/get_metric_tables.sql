{% macro get_metric_tables() %}
  SELECT
    T.table_name,
    CONCAT('`', T.table_catalog, '`.`', T.table_schema, '`.`', T.table_name, '`') AS fully_qualified_bq_table
  FROM {{target.project}}.{{target.dataset}}.INFORMATION_SCHEMA.TABLES AS T
  JOIN {{ ref('metric_all_definitions') }} AS D ON T.table_name = D.metric_name
{%- endmacro %}