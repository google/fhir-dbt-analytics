{%- macro linking_api_datasource(report_id, datasource_list) -%}
  CONCAT(
    'https://lookerstudio.google.com/reporting/create?',
    'c.reportId=', '{{ report_id|urlencode }}',
    {%- for datasource in datasource_list %}
      {%- set alias = datasource[0] -%}
    '&ds.{{ alias }}.type=TABLE',
    '&ds.{{ alias }}.projectId=', '{{ this.database|urlencode }}',
    '&ds.{{ alias }}.datasetId=', '{{ this.schema|urlencode }}',
    '&ds.{{ alias }}.tableId=', '{{ datasource[1]|urlencode }}'
    {%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
  )
{%- endmacro -%}

