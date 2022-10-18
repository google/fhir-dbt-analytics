{% macro try_has_code_from_code_system(field_name, code_system, fhir_resource=None, index=None) -%}

    {%- if execute and fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='primary_resource') -%}
    {%- endif -%}

    {# See if coding.code and coding.system columns exists in resource #}
    {%- if column_exists(fhir_resource=fhir_resource, column_name=field_name~'.coding.code')
      and column_exists(fhir_resource=fhir_resource, column_name=field_name~'.coding.system') -%}
        {# Check if you want a particular index of the field or not #}
        {%- if index is not none -%}
            {%- set field_name_idx = field_name~'[SAFE_OFFSET('~index~')]' -%}
            {{ return(has_code_from_code_system(field_name_idx, code_system)) }}
        {%- else -%}
            {{ return(has_code_from_code_system(field_name, code_system)) }}
        {%- endif -%}
    {# If either coding.code or coding.system does not exist, return 0 #}
    {%- else -%}
        {{ return(0) }}
    {%- endif -%}
{%- endmacro %}