{%- macro is_public_synthea_dataset() -%}
    {{ return(var('schema') == 'fhir_synthea' and var('database') == 'bigquery-public-data') }}
{%- endmacro -%}