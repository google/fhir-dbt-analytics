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

    {{ return(columns if columns|length else None) }}
{%- endmacro -%}