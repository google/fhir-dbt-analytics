{% macro column_data_type(column_name, source_table) %}

    {%- set relation = adapter.get_relation(
          database = var('database'),
          schema = var('schema'),
          identifier = source_table
        )
    -%}

    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {%- for top_level_column in columns -%}
        {% for column in top_level_column.flatten() %}
            {% if column_name == column.name %}
                {% do return (column.data_type) %}
            {% endif %}
        {%- endfor -%}
    {%- endfor -%}

{% endmacro %}
