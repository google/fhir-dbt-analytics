{% macro get_dbt_objects(resource_type=None) -%}
{%- set models_dict = {} -%}
{% if execute %}
  {% if resource_type!=None %}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", resource_type) %}
      {% do models_dict.update({node.name: node.path}) %}
    {% endfor %}
  {% else %}
    {% for node in graph.nodes.values() %}
      {% do models_dict.update({node.name: node.path}) %}
    {% endfor %}
  {% endif %}
{% endif %}
{% do return(models_dict) %}
{%- endmacro -%}
