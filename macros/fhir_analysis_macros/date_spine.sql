{% macro date_spine() -%}
  {{ return(adapter.dispatch('date_spine', 'fhir_dbt_analytics') ()) }}
{%- endmacro %}


{% macro default__date_spine() -%}

{%- if var('static_dataset') -%}
  {% set start_date="cast('" ~ var('earliest_date') ~ "' as date)" %}
  {% set end_date="cast('" ~ var('latest_date') ~ "' as date)" %}
{%- else -%}
  {% set start_date=dbt_date.n_months_ago(var('months_history'), tz=data_timezone()) %}
  {% set end_date=dbt_date.today(tz=data_timezone()) %}
{%- endif -%}

-- ---------------------- start of date_spine -----------------------
with date_spine as (
  {{ dbt_utils.date_spine(datepart="day", start_date=start_date, end_date=end_date) }}
)
{# dbt_utils cast to datetime, but we want date #}
select cast(date_day as date) as date_day from date_spine
-- ---------------------- end of date_spine -------------------------
{% endmacro %}


{% macro bigquery__date_spine() -%}
(SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(
{%- if var('static_dataset') %}
  "{{ var('earliest_date') }}", "{{ var('latest_date') }}"
{%- else %}
  DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('months_history') }} MONTH),
  CURRENT_DATE()
{%- endif %}
)) AS date_day)
{%- endmacro %}
