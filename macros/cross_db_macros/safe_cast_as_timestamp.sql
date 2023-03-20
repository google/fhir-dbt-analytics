{%- macro safe_cast_as_timestamp(x) -%}
    {{ dbt.safe_cast(x, api.Column.translate_type("timestamp")) }}
{%- endmacro -%}
