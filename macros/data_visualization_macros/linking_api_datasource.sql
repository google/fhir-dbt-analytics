{%- macro linking_api_datasource(report_id, datasource_list) -%}
  CONCAT(
    'https://lookerstudio.google.com/reporting/create?',
    'c.reportId=', '{{ report_id|urlencode }}',
    {%- for datasource in datasource_list %}
    '&ds.mbssd.type=TABLE',
    '&ds.mbssd.projectId=', '{{ this.database|urlencode }}',
    '&ds.mbssd.datasetId=', '{{ this.schema|urlencode }}',
    '&ds.mbssd.tableId=', '{{ datasource|urlencode }}'
    {%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
  )
{%- endmacro -%}