-- Run using:
-- dbt run-operation run_unit_tests
{% macro run_unit_tests() %}
    {% do test_quote_array() %}
    {% do test_snake_case() %}
    {% do test_sql_comparison_expression() %}
    {% do test_flatten_column() %}
    {% do test_unnest() %}
    {% do test_unnest_multiple() %}
    {% do test_select_from_unnest() %}
    {% do test_code_from_codeableconcept() %}
{% endmacro %}

