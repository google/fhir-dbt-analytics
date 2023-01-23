{% macro column_exists(column_name, fhir_resource=None) %}

    {%- if not execute -%}
        {% do return(True) %}
    {% endif %}

    {%- if fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='fhir_resource') -%}
    {%- endif -%}
    {%- if fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='primary_resource') -%}
    {%- endif -%}

    {# Initialise object for table based on names rather than ref() #}
    {%- set relation = adapter.get_relation(
        database = var('database'),
        schema = var('schema'),
        identifier = get_source_table_name(fhir_resource)) -%}

    {% if not relation %}
        {% do return (False) %}
    {% endif %}

    {# Get columns in this table #}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {% for top_level_column in columns %}
        {% for column in top_level_column.flatten() %}
          {% if column_name == column.name %}
            {% do return (True) %}
          {% endif %}
        {% endfor %}
    {% endfor %}

    {% do return (False) %}

{% endmacro %}