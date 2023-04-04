{% macro table_ref(database, schema, table) -%}
  {{ return(adapter.dispatch('table_ref', 'fhir_dbt_analytics') (database, schema, table)) }}
{%- endmacro %}


{% macro default__table_ref(database, schema, table) -%}
  {{ return("`"~database~"`.`"~schema~"`.`"~table~"`") }}
{%- endmacro %}


{% macro spark__table_ref(database, schema, table) -%}
  {{ return(schema~"."~table) }}
{%- endmacro %}