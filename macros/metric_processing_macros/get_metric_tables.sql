{% macro get_metric_tables() %}
  {{ return(adapter.dispatch('get_metric_tables', 'fhir_dbt_analytics') ()) }}
{% endmacro %}


{% macro bigquery__get_metric_tables() %}
  {% set metric_tables %}
  SELECT T.table_name
  FROM {{target.project}}.{{target.dataset}}.INFORMATION_SCHEMA.TABLES AS T
  JOIN {{ ref('metric_all_definitions') }} AS D ON T.table_name = D.metric_name
  {% endset %}

  {% set metric_names = run_query(metric_tables).columns[0].values() %}
  {% set metric_dict = {} %}
  {% for metric in metric_names %}
    {{ metric_dict.update({ metric : table_ref(target.project, target.schema, metric) } )}}
  {% endfor %}

  {{ return(metric_dict) }}
{% endmacro %}


{% macro spark__get_metric_tables() %}
  {% if not execute %}
    {{ return (None) }}
  {% endif %}
  {% set show_tables %}
    SHOW TABLES IN {{target.schema}}
  {% endset %}
  {% set metrics = run_query(show_tables).columns[1].values() %}
  {% set metric_dict = {} %}
  {% for node in graph.nodes.values()
     if node.resource_type == 'model'
     and node.path.startswith('metrics/')
     and node.name in metrics %}
     {{ metric_dict.update({ node.name : table_ref("", target.schema, node.name) }) }}
  {% endfor %}
  {{ return (metric_dict) }}
{% endmacro %}
