{% macro spark__init_sources(fhir_resources, parquet_location) %}
  {% set sql = "CREATE DATABASE IF NOT EXISTS "~var('schema') %}
  {{ print(sql) }}
  {{ run_query(sql) }}

  {% for fhir_resource in fhir_resources.split(",") %}
    {% set location = parquet_location~"/"~(fhir_resource|trim|lower) %}
    {% set sql = "CREATE TABLE IF NOT EXISTS "~var('schema')~"."~(fhir_resource|trim)
                ~" USING PARQUET LOCATION '"~location~"'" %}
    {{ print(sql) }}
    {{ run_query(sql) }}
  {% endfor %}
{% endmacro %}
