-- Copyright 2023 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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

