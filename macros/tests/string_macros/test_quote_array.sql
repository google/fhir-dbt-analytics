{% macro test_quote_array() -%}
  {{ dbt_unittest.assert_equals(quote_array([]), []) }}
  {{ dbt_unittest.assert_equals(quote_array(["foo"]), ["'foo'"]) }}
  {{ dbt_unittest.assert_equals(quote_array(["foo", "bar"]), ["'foo'", "'bar'"]) }}

  {{ return(adapter.dispatch('test_quote_array', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}


{% macro default__test_quote_array() -%}
  {{ dbt_unittest.assert_equals(quote_array(["f'o'o"]), ["'f\\'o\\'o'"]) }}
{%- endmacro %}


{% macro spark__test_quote_array() -%}
  {{ dbt_unittest.assert_equals(quote_array(["f'o'o"]), ["'f''o''o'"]) }}
{%- endmacro %}
