{% macro get_column_or_default(column_name, fhir_resource=None, table_alias=None) %}

    {%- if execute and fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='primary_resource') -%}
    {%- endif -%}

    {%- if column_exists(column_name, fhir_resource) -%}
        {%- if table_alias != None -%}
          {{ return(table_alias ~ "." ~ column_name) }}
        {%- else -%}
          {{ return(column_name) }}
        {%- endif -%}
    {%- else -%}
        {{ return('NULL') }}
    {%- endif -%}

{% endmacro %}