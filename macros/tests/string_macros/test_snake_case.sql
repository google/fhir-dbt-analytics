{% macro test_snake_case() %}

    {{ dbt_unittest.assert_equals(snake_case("abc"), "abc") }}
    {{ dbt_unittest.assert_equals(snake_case("Abc"), "abc") }}
    {{ dbt_unittest.assert_equals(snake_case("AbC"), "ab_c") }}
    {{ dbt_unittest.assert_equals(snake_case("foo_bar"), "foo_bar") }}
    {{ dbt_unittest.assert_equals(snake_case("FooBar"), "foo_bar") }}
    {{ dbt_unittest.assert_equals(snake_case("FoObAr"), "fo_ob_ar") }}

{% endmacro %}