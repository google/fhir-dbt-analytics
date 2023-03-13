{% macro test_quote_array() %}

    {{ dbt_unittest.assert_equals(quote_array([]), []) }}
    {{ dbt_unittest.assert_equals(quote_array(["foo"]), ["'foo'"]) }}
    {{ dbt_unittest.assert_equals(quote_array(["foo", "bar"]), ["'foo'", "'bar'"]) }}
    {{ dbt_unittest.assert_equals(quote_array(["f'o'o"]), ["'f\\'o\\'o'"]) }}

{% endmacro %}