{% macro column_exists(column_name, fhir_resource=None) %}

    {%- if not execute -%}
        {% do return(True) %}
    {% endif %}

    {% set column_dict = get_column_datatype_dict(fhir_resource) %}

    {% do return(column_name in column_dict) %}

{% endmacro %}