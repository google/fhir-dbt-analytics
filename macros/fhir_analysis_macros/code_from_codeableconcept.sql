{% macro code_from_codeableconcept(
  field_name,
  code_system,
  fhir_resource=None,
  index=None,
  null_values=var('null_values'),
  return_field='code',
  return_int=False
) -%}

{%- if execute and fhir_resource == None -%}
  {%- set fhir_resource = model_metadata(meta_key='primary_resource') -%}
{%- endif -%}

{%- set column_dict = get_column_datatype_dict(fhir_resource) -%}

{%- if execute and (
      field_name ~ '.coding.code' not in column_dict or
      field_name ~ '.coding.system' not in column_dict
    )
-%}
  {{ return(0) if return_int else return("'missing_or_invalid_codeableconcept_field'") }}
{%- endif -%}

{%- set field_is_array =
      execute
      and field_name in column_dict
      and column_dict[field_name].startswith('ARRAY')
-%}

{%- if index != None -%}
  {%- set field_name = field_name~'[SAFE_OFFSET('~index~')]' -%}
{%- endif -%}

{%- if return_int == True %}
      (
        SELECT SIGN(COUNT(*))
        {%- if field_is_array and index==None %}
        FROM UNNEST({{field_name}}) f, UNNEST(f.coding) c
        {%- else %}
        FROM UNNEST({{field_name}}.coding) AS c
        {%- endif %}
        WHERE c.system = '{{code_system}}'
        AND c.code IS NOT NULL
        {%- if null_values|length > 0 %}
        AND c.code NOT IN ('{{null_values|join("', '")}}')
        {%- endif %}
      )
{%- else -%}
  {%- if field_is_array and index==None %}
    (SELECT c.{{return_field}} FROM UNNEST({{field_name}}) f, UNNEST(f.coding) c WHERE c.system = '{{code_system}}' ORDER BY c.code LIMIT 1)
  {%- else -%}
    (SELECT c.{{return_field}} FROM UNNEST({{field_name}}.coding) c WHERE c.system = '{{code_system}}' ORDER BY c.code LIMIT 1)
  {%- endif -%}
{%- endif -%}

{%- endmacro %}