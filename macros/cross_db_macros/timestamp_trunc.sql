{% macro timestamp_trunc(datepart, date, timezone=None) -%}
  {{ return(adapter.dispatch('timestamp_trunc', 'fhir_dbt_analytics') (datepart, date, timezone)) }}
{%- endmacro %}


{% macro bigquery__timestamp_trunc(datepart, date, timezone) -%}
  TIMESTAMP_TRUNC({{ date }}, {{ datepart }}, {{ timezone }})
{%- endmacro %}


{% macro default__timestamp_trunc(datepart, date, timezone) -%}
  {{ dbt.date_trunc(datepart, date) }}
{%- endmacro %}