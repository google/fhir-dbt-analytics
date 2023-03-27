{% macro get_column_datatype_dict(fhir_resource=None) %}

    {%- if not execute -%}
        {% do return({}) %}
    {% endif %}

    {%- if fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='fhir_resource') -%}
    {%- endif -%}
    {%- if fhir_resource == None -%}
        {%- set fhir_resource = model_metadata(meta_key='primary_resource') -%}
    {%- endif -%}

    {%- set relation = adapter.get_relation(
        database = this.project,
        schema = this.schema,
        identifier = fhir_resource ~ "_view") -%}

    {% if not relation %}
        {% do return ({}) %}
    {% endif %}

    {%- set column_dict = {} -%}

    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {% for top_level_column in columns %}
        {%- do column_dict.update({top_level_column.name: top_level_column.data_type}) -%}
        {% for column in flatten_column(top_level_column) %}
            {%- do column_dict.update({column.name: column.data_type}) -%}
        {% endfor %}
    {%- endfor -%}

    {%- do return(column_dict) -%}

{% endmacro %}