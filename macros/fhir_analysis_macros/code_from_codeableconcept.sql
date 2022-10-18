{% macro code_from_codeableconcept(field_name, code_system, return_display=False) -%}
  {%- if return_display == True -%}
    (SELECT display FROM UNNEST({{field_name}}.coding) WHERE system = '{{code_system}}')
  {%- else -%}
    (SELECT code FROM UNNEST({{field_name}}.coding) WHERE system = '{{code_system}}')
  {%- endif -%}
{%- endmacro %}