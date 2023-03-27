-- Run using:
-- dbt run-operation run_unit_tests
{% macro run_unit_tests() %}
    {% do test_snake_case() %}
    {% do test_sql_comparison_expression() %}
    {% do test_flatten_column() %}
{% endmacro %}

