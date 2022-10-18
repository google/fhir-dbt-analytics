{%- macro fhir_resource_view_expression() -%}

{%- if execute -%}
    {%- set fhir_resource = model_metadata(meta_key='fhir_resource') -%}
{%- else %}
    {%- set fhir_resource = 'N/A' -%}
{%- endif -%}

{%- if fhir_resource_exists(fhir_resource) -%}
{{ query_fhir_table(fhir_resource) }}
{%- else %}
{{ create_dummy_table() }}
{%- endif -%}

{%- endmacro -%}