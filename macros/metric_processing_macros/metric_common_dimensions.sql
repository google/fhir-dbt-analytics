{%- macro metric_common_dimensions(table_alias=None, exclude_col=None) -%}

{%- set exclude_cols = [exclude_col] -%}
{%- set common_dimensions = [
  'fhir_mapping',
  'source_system',
  'data_transfer_type',
  'metric_date',
  'site'
] -%}
{%- set columns = common_dimensions | reject('in', exclude_cols) -%}

{%- if table_alias != None -%}
  {%- set prefix = table_alias ~ '.' -%}
{%- else -%}
  {%- set prefix = '' -%}
{%- endif -%}

{%- for col in columns %}
  {{ prefix ~ col }},
{%- endfor -%}

{%- endmacro -%}