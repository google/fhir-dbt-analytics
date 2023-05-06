{% macro test_unnest_multiple() -%}
  {{ return(adapter.dispatch('test_unnest_multiple', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}


{% macro default__test_unnest_multiple() -%}
  {{ assert_string_equals(
      unnest_multiple(
        [array_config( field = 'my_array', unnested_alias = 'c' )],
      ),
  "UNNEST(my_array) c") }}

  {{ assert_string_equals(
      unnest_multiple(
        [array_config( field = 'my_array_2', unnested_alias = 'm' ),
         array_config( field = 'm.foos', unnested_alias = 'd' )],
      ),
  "UNNEST(my_array_2) m, UNNEST(m.foos) d") }}
{%- endmacro %}


{% macro spark__test_unnest_multiple() -%}
  {{ assert_string_equals(
      spark__unnest_multiple(
        arrays = [array_config( field = 'my_array', unnested_alias = 'c' )],
      ),
  "SELECT * FROM (SELECT EXPLODE(ac) AS c FROM (SELECT my_array AS ac))") }}

  {{ assert_string_equals(
      spark__unnest_multiple(
        arrays = [
          array_config( field = 'my_array_2', unnested_alias = 'm' ),
          array_config( field = 'm.foos', unnested_alias = 'd' )],
      ),
  "SELECT * FROM (SELECT EXPLODE(ac) AS m FROM (SELECT my_array_2 AS ac))
    LATERAL VIEW OUTER explode (m.foos) AS d
  )") }}

{% endmacro %}
