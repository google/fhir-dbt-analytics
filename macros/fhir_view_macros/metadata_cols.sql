{% macro metadata_cols() %}
    {{ source_system() }} AS source_system,
    {{ site() }} AS site,
    {{ data_transfer_type() }} AS data_transfer_type
{% endmacro %}