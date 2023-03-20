{%- macro is_public_synthea_dataset() -%}
    {# When importing the public Synthea data to Spark, name the schema 'public_synthea'. #}
    {{ return(
        (var('schema') == 'public_synthea' and var('database') == '') or
        (var('schema') == 'fhir_synthea' and var('database') == 'bigquery-public-data')
       ) }}
{%- endmacro -%}