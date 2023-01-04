{% macro model_metadata(meta_key, model_name=this.name, value_if_missing=None) %}

  {%- if execute -%}
    {% set model_id = "model." ~ project_name ~ "." ~ model_name %}
    {% set meta_value = graph["nodes"][model_id]["meta"][meta_key] %}
  {%- endif -%}

  {% do return(meta_value if meta_value else value_if_missing) %}
{% endmacro %}