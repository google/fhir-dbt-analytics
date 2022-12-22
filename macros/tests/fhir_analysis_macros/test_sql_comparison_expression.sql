{% macro test_sql_comparison_expression() %}

    {{ dbt_unittest.assert_equals(
      sql_comparison_expression("abc"),
      "= 'abc'") }}

    {{ dbt_unittest.assert_equals(
      sql_comparison_expression(["abc", "foo"]),
      "IN ('abc', 'foo')") }}

{% endmacro %}