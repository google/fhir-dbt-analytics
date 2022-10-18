{%- macro get_metric_date_columns() -%}
    {% set columns = model_metadata('metric_date_columns') -%}
    {% if is_public_synthea_dataset() %}
        {% if columns == ['recordedDate'] %}
            {{ return(['assertedDate']) }}
        {% endif %}
    {% endif %}
    {{ return(columns) }}
{%- endmacro -%}