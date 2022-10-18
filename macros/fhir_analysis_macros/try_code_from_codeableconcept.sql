{% macro try_code_from_codeableconcept(field_name, code_system, fhir_resource=None, index=None, return_display=False) -%}

    {%- if execute and fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='primary_resource') -%}
    {%- endif -%}

    {# See if coding.code and coding.system columns exists in resource #}
    {%- if column_exists(fhir_resource=fhir_resource, column_name=field_name~'.coding.code')
      and column_exists(fhir_resource=fhir_resource, column_name=field_name~'.coding.system') -%}
        {# Check if you want a particular index of the field or not #}
        {%- if index is not none -%}
            {%- set field_name_idx = field_name~'[SAFE_OFFSET('~index~')]' -%}
            {{ return(code_from_codeableconcept(field_name_idx, code_system, return_display)) }}
        {%- else -%}
            {{ return(code_from_codeableconcept(field_name, code_system, return_display)) }}
        {%- endif -%}
    {# If either coding.code or coding.system does not exist, return "UNK" #}
    {%- else -%}
        {{ return("'UNK'") }}
    {%- endif -%}
{%- endmacro %}