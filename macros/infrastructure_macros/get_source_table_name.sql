{%- macro get_source_table_name(fhir_resource) -%}
    {% if execute and is_public_synthea_dataset() %}
        {{ return(snake_case(fhir_resource)) }}
    {% else %}
        {{ return(fhir_resource) }}
    {% endif %}
{%- endmacro -%}