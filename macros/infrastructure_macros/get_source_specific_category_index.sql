{%- macro get_source_specific_category_index() -%}
    {% if is_public_synthea_dataset() %}
        {{ return(None) }}
    {% else %}
        {{ return(0) }}
    {% endif %}
{%- endmacro -%}