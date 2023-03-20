{%- macro safe_cast_as_date(x) -%}
    {{ dbt.safe_cast(x, api.Column.translate_type("date")) }}
{%- endmacro -%}
