{% macro age(date_of_birth='birthDate') -%}
 DATE_DIFF({{get_snapshot_date()}}, DATE({{date_of_birth}}), YEAR) - IF(EXTRACT(DAYOFYEAR FROM DATE({{date_of_birth}})) > EXTRACT(DAYOFYEAR FROM DATE({{get_snapshot_date()}})), 1, 0)
{%- endmacro %}