{%- macro date_array() -%}
GENERATE_DATE_ARRAY(
{%- if var('static_dataset') %}
  PARSE_DATE("%F", "{{ var('earliest_date') }}"),
  PARSE_DATE("%F", "{{ var('latest_date') }}")
{%- else %}
  DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('months_history') }} MONTH),
  CURRENT_DATE()
{%- endif %}
)
{%- endmacro -%}