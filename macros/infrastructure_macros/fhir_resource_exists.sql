{%- macro fhir_resource_exists(test_fhir_resource) -%}

    {# Query all available FHIR resources #}
    {% set resource_list =
        dbt_utils.get_column_values(table=ref('fhir_table_list'), column='fhir_resource') %}

    {# Check for resource of interest #}
    {% for resource in resource_list %}
       {% if get_source_table_name(resource) == get_source_table_name(test_fhir_resource) %}
          {{ return(True) }}
       {% endif %}
    {% endfor %}

    {{ return(False) }}

{% endmacro %}