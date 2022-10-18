{% macro get_snapshot_date()-%}
  {%- if var('cohort_snapshot_date') == 'today' -%}
    {%- do return('CURRENT_DATE()') -%}
  {%- else -%}
    {%- do return("'" ~ var('cohort_snapshot_date') ~ "'") -%}
  {%- endif -%}
{%- endmacro %}