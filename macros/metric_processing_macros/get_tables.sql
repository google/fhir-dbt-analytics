{% macro get_tables(table_name_wildcard="%") %}
  SELECT
    T.table_name,
    CONCAT('`', T.table_catalog, '`.`', T.table_schema, '`.`', T.table_name, '`') AS fully_qualified_bq_table
  FROM {{target.project}}.{{target.dataset}}.INFORMATION_SCHEMA.TABLES AS T
  WHERE T.table_name LIKE '{{ table_name_wildcard }}'
{%- endmacro %}