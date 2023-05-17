{% macro test_unnest() -%}
  {{ return(adapter.dispatch('test_unnest', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}


{% macro default__test_unnest() -%}
  {{ assert_string_equals(
      unnest('my_array', 'c' ),
      "UNNEST(my_array) c") }}

  {{ assert_string_equals(
      unnest('my_array' ),
      "UNNEST(my_array)") }}
{%- endmacro %}


{% macro spark__test_unnest() -%}
  {{ assert_string_equals(
        spark__unnest('my_array', 'c' ),
        "SELECT EXPLODE(ac) AS c FROM (SELECT my_array AS ac)") }}
  {{ assert_string_equals(
        spark__unnest('my_array'),
        "SELECT EXPLODE(ac) FROM (SELECT my_array AS ac)") }}
{%- endmacro %}
