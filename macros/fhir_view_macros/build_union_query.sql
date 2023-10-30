{% macro build_union_query(maps) %}

{%- set relations = raw_tables_to_relations(maps) %}
{%- set relation_columns = {} -%}
{%- set column_superset = {} -%}
{%- set column_data_type_clash = [] -%}
{%- set metric_date_columns = get_metric_date_columns() -%}

{#- Iterate through tables to produce superset of all columns -#}
{%- for relation in relations -%}
  {%- set columns = adapter.get_columns_in_relation(relation) -%}
  {%- do relation_columns.update({relation: []}) -%}
  {%- for col in columns -%}
    {%- do relation_columns[relation].append(col.column) -%}
      {% for sub_col in flatten_column(col) %}
        {% do relation_columns[relation].append(sub_col.column) %}
      {%- endfor -%}
    {%- if col.column not in column_superset -%}
      {%- do column_superset.update({col.column: col.data_type}) -%}
    {%- else -%}
      {%- set data_type_existing = column_superset[col.column] -%}
      {%- if col.data_type != data_type_existing %}
        {%- do column_data_type_clash.append(col.column) -%}
      {%- endif -%}
      {%- do column_superset.update({col.column: col.data_type}) -%}
    {%- endif -%}
  {%- endfor -%}
{%- endfor -%}

{#- Iterate through tables to create a select statement per table -#}
{%- for relation in relations %}

    {#- Filter list of metric date columns to only those that are mapped in table -#}
    {%- if metric_date_columns != None %}
      {%- for date_column in metric_date_columns %}
        {%- do metric_date_columns.remove(date_column) if date_column not in relation_columns[relation] -%}
      {%- endfor %}
    {%- endif %}
    {%- if metric_date_columns == [] %}
      {%- set metric_date_columns = None %}
    {%- endif %}

SELECT
  {#- Iterate through columns that exist in all tables for this FHIR resource -#}
  {%- for column in column_superset %}
    {%- if column in column_data_type_clash %}
    'subfield_mismatch' AS {{ column }},
    {%- else -%}
    {%- set col_name = adapter.quote(column) if column in relation_columns[relation] else 'null' %}
    {{ col_name }} AS {{ column }},
    {%- endif -%}
  {%- endfor -%}

    {#- Add additional derived columns to the view #}
    '{{ relation.identifier }}' AS fhir_mapping,
    {{ metric_date(metric_date_columns, column_superset[metric_date_columns]) }} AS metric_date,
    {{ metric_hour(metric_date_columns, column_superset[metric_date_columns]) }} AS metric_hour,
    {%- if 'meta.sourceIdentifier.system' in relation_columns[relation] %}
    {{ source_system() }} AS source_system,
    {{ site() }} AS site,
    {%- else %}
    '{{ var('source_system_default') }}' AS source_system,
    '{{ var('site_default') }}' AS site,
    {%- endif -%}
    {%- if 'meta.dataTransferType' in relation_columns[relation] %}
    {{ data_transfer_type() }} AS data_transfer_type
    {%- else %}
    '{{ var('data_transfer_type_default') }}' AS data_transfer_type
    {%- endif %}
FROM {{ relation }}
{%- if not loop.last %}
UNION ALL
{%- endif -%}
{%- endfor -%}

{% endmacro %}