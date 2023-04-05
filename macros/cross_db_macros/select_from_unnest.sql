{% macro select_from_unnest(select, unnested, where="1=1", order_by="1") -%}
  {{ return (adapter.dispatch('select_from_unnest', 'fhir_dbt_analytics') (select, unnested, where, order_by)) }}
{%- endmacro %}


{% macro default__select_from_unnest(select, unnested, where, order_by) -%}
SELECT {{ select }} FROM {{ unnested }} WHERE {{ where }} ORDER BY {{ order_by }} LIMIT 1
{%- endmacro %}


{% macro spark__select_from_unnest(select, unnested, where, order_by) -%}
SELECT ELEMENT_AT(COLLECT_LIST({{ select }}), 1) FROM (
  SELECT * FROM ({{ unnested }})
  WHERE {{ where }}
  ORDER BY {{ order_by }})
{%- endmacro %}
