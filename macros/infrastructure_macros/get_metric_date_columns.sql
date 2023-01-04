{%- macro get_metric_date_columns() -%}
    {% set columns = model_metadata('metric_date_columns') -%}
    {% if not columns %}
        {{ return(None) }}
    {% endif %}

    {% if is_public_synthea_dataset() %}
        {% if columns == ['recordedDate'] %}
            {{ return(['assertedDate']) }}
        {% endif %}
    {% endif %}

    {%- set existing_columns = [] -%}
    {%- for column in columns if column_exists(column) -%}
        {{ existing_columns.append(column) }}
    {%- endfor -%}

    {{ return(existing_columns if existing_columns|length else None) }}
{%- endmacro -%}