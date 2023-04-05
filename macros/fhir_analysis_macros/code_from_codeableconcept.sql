# After replacing all references of `code_from_codeableconcept` by a combination of `unnest` and
# this macro, rename this to `code_from_codeableconcept`.
{% macro code_from_codeableconcept(
  field_name,
  code_system,
  fhir_resource=None,
  index=None,
  null_values=var('null_values'),
  return_field='code',
  return_int=False
) %}

{% if execute and fhir_resource == None %}
  {% set fhir_resource = model_metadata(meta_key='primary_resource') %}
{% endif %}

{% set column_dict = get_column_datatype_dict(fhir_resource) %}

{% if execute and (
      field_name~'.coding.code' not in column_dict or
      field_name~'.coding.system' not in column_dict
    )
%}
  {{ return(0) if return_int else return("'missing_or_invalid_codeableconcept_field "~field_name~"'") }}
{% endif %}

{% set field_is_array =
      execute
      and field_name in column_dict
      and column_dict[field_name].startswith('ARRAY')
%}

{% if index != None %}
  {% set field_name = safe_offset(field_name, index) %}
{% endif %}

{{ return(_code_from_codeableconcept(
  field_name=field_name,
  code_system=code_system,
  null_values=null_values,
  return_field=return_field,
  return_int=return_int,
  field_is_array=field_is_array and index==None)) }}
{% endmacro %}


{# Separate the internal part to support testing. #}
{% macro _code_from_codeableconcept(
  field_name,
  code_system,
  field_is_array,
  null_values,
  return_field,
  return_int
) %}
{%- set alias = "c" %}
{%- if field_is_array %}
  {%- set arrays=[
    array_config(field = field_name, unnested_alias = "f"),
    array_config(field = "f.coding", unnested_alias = alias)] %}
{%- else %}
  {%- set arrays=[array_config(field = field_name~".coding", unnested_alias = alias)] %}
{%- endif %}

{%- if return_int -%}
      (
        SELECT SIGN(COUNT(*))
        FROM {{ unnest_multiple(arrays) }}
        WHERE {{alias}}.system = '{{code_system}}'
        AND {{alias}}.code IS NOT NULL
        {%- if null_values|length > 0 %}
        AND {{alias}}.code NOT IN ('{{null_values|join("', '")}}')
        {%- endif %}
      )
{%- else -%}
  ({{ select_from_unnest(
      select = alias~"."~return_field,
      unnested = unnest_multiple(arrays),
      where = alias~".system = '"~code_system~"'",
      order_by = alias~".code") }})
{%- endif %}

{%- endmacro %}