{% macro get_maps_for_resource(fhir_resource) %}

    {# Build query to check for all mappings of a certain resource #}
    {%- call statement('result', fetch_result=True) -%}
    SELECT DISTINCT bq_table
    FROM {{ ref('fhir_table_list') }} AS L
    WHERE fhir_resource = '{{ fhir_resource }}'
    AND latest_version = 1
    {%- endcall -%}

    {# Return result, or dummy array of ['Observation'] if result is empty #}
    {% if execute %}
        {{ return(load_result('result').table.columns[0].values()) }}
    {% else %}
        {{ return(['Observation']) }}
    {% endif %}

{% endmacro %}