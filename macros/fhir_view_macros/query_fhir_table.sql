{%- macro query_fhir_table(fhir_resource) -%}
    {%- set metric_date_columns = get_metric_date_columns() -%}
    {%- set date_column_data_type = column_data_type(metric_date_columns[0], get_source_table_name(fhir_resource)) %}
SELECT
    *,
    CAST(NULL AS STRING) AS fhir_mapping,
    {{- metric_date(metric_date_columns, date_column_data_type) }} AS metric_date,
    {{- metadata_cols() -}}
FROM {{ source('fhir', get_source_table_name(fhir_resource)) }}
{% endmacro %}