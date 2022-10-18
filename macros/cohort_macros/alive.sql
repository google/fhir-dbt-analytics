{% macro alive() -%}
 (P.deceased.dateTime IS NULL OR DATE(P.deceased.dateTime) > {{get_snapshot_date()}})
{%- endmacro %}}}