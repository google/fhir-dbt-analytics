{% macro column_exists(column_name, fhir_resource=None) %}

    {%- if execute and fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='primary_resource') -%}
    {%- endif -%}

    {# Initialise object for table based on names rather than ref() #}
    {%- set relation = adapter.get_relation(
        database = this.project,
        schema = this.dataset,
        identifier = fhir_resource ~ "_view") -%}

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