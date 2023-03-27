{% macro column_data_type(column_name, source_table) %}

    {%- set relation = adapter.get_relation(
          database = var('database'),
          schema = var('schema'),
          identifier = source_table
        )
    -%}

    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {%- for top_level_column in columns -%}
        {% if column_name == top_level_column.name %}
            {% do return (top_level_column.data_type) %}
        {% endif %}
        {% for column in flatten_column(top_level_column) %}
            {% if column_name == column.name %}
                {% do return (column.data_type) %}
            {% endif %}
        {%- endfor -%}
    {%- endfor -%}

{% endmacro %}
